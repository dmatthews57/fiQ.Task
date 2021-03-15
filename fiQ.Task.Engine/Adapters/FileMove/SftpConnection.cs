using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Renci.SshNet;

namespace fiQ.TaskAdapters.FileMove
{
	class SftpConnection : Connection
	{
		#region Fields
		private SftpClient sftpClient = null;
		private string basepath = null;
		private bool disposed = false;
		#endregion

		#region IDisposable implementation
		protected override void Dispose(bool disposing)
		{
			if (disposed == false && disposing && sftpClient != null)
			{
				sftpClient.Dispose();
				sftpClient = null;
			}
			disposed = true;
			base.Dispose(disposing);
		}
		#endregion

		#region Connection implementation - Connection management
		/// <summary>
		/// Connection function - create SftpClient and connect to server
		/// </summary>
		protected override void DoConnect()
		{
			// SftpClient will throw unfriendly error for this specific case; check first:
			if (string.IsNullOrEmpty(config.userID))
			{
				throw new ArgumentException("SFTP UserID required");
			}

			// Create URL for destination location and save scrubbed base path, removing leading slash so path is by default
			// relative to home folder - note URI can still be configured with "//" after hostname in order to force path from
			// root folder, if required (i.e. sftp://hostname/relativetohome vs sftp://hostname//relativetoroot"):
			var uri = new Uri(config.location);
			basepath = ScrubPath(uri.AbsolutePath);
			if (basepath.StartsWith('/'))
			{
				basepath = basepath[1..];
			}
			// Unless basepath is empty, ensure it ends with forward slash:
			if (!string.IsNullOrEmpty(basepath) && !basepath.EndsWith('/'))
			{
				basepath += "/";
			}

			PrivateKeyFile privateKeyFile = null;
			var methods = new AuthenticationMethod[1];
			try
			{
				// Populate authentication method array with password or private key method:
				if (string.IsNullOrEmpty(config.clientCertificate))
				{
					methods[0] = new PasswordAuthenticationMethod(config.userID, config.password ?? string.Empty);
				}
				else
				{
					privateKeyFile = new PrivateKeyFile(config.clientCertificate, config.password);
					methods[0] = new PrivateKeyAuthenticationMethod(config.userID, new[] { privateKeyFile });
				}

				// Create SftpClient and attempt connection:
				sftpClient = new SftpClient(new ConnectionInfo(uri.Host, (uri.Port == -1 ? 22 : uri.Port), config.userID, methods));
				sftpClient.Connect();
			}
			finally
			{
				// Clean up connection resources:
				if (privateKeyFile != null)
				{
					privateKeyFile.Dispose();
				}
				if (methods[0] is IDisposable disposable)
				{
					disposable.Dispose();
				}
			}
		}
		/// <summary>
		/// Disconnection function - gracefully disconnect from server, if connected
		/// </summary>
		protected override void DoDisconnect()
		{
			if (sftpClient != null)
			{
				sftpClient.Disconnect();
			}
		}
		#endregion

		#region Connection implementation - File transfer
		/// <summary>
		/// Retrieve listing of downloadable files
		/// </summary>
		/// <param name="paths">Collection of SourceFilePaths to search</param>
		/// <returns>HashSet of downloadable files in source paths, matching source </returns>
		/// <remarks>Used only when this is "source" connection</remarks>
		public override HashSet<DownloadFile> GetFileList(List<SourceFilePath> paths)
		{
			var fileset = new HashSet<DownloadFile>();
			foreach (var path in paths)
			{
				string subfolderPath = GetSubfolderPath(path.FolderPath);
				if (string.IsNullOrEmpty(subfolderPath) || sftpClient.Exists(subfolderPath))
				{
					// Construct Regex for filename filter (required), and for filename regex (optional):
					var fileFilterRegex = TaskUtilities.General.RegexFromFileFilter(path.FilenameFilter);
					var fileNameRegex = TaskUtilities.General.RegexIfPresent(path.FilenameRegex, RegexOptions.IgnoreCase);

					fileset.UnionWith(
						// Search current path if no subfolder specified:
						sftpClient.ListDirectory(string.IsNullOrEmpty(subfolderPath) ? "." : subfolderPath)
							.Where(sf =>
								sf.IsDirectory == false
								&& fileFilterRegex.IsMatch(sf.Name)
								&& (fileNameRegex == null ? true : fileNameRegex.IsMatch(sf.Name))
							)
							.Select(sf => new DownloadFile
							{
								// Note that actual server path (including forward slash) is replacing configured FolderPath
								// value here - so later operations can just use it directly without re-evaluating:
								fileFolder = subfolderPath,
								fileName = sf.Name,
								lastWriteTime = sf.LastWriteTime,
								size = sf.Length,
								DestinationSubfolder = path.DestinationSubfolder
							})
					);
				}
			}
			return fileset;
		}
		/// <summary>
		/// Open writable stream for specified destination file
		/// </summary>
		/// <remarks>Used only when this is "destination" connection</remarks>
		public override StreamPath GetWriteStream(string folderPath, string fileName, bool preventOverwrite)
		{
			// Ensure base folder (if any) exists:
			if (!string.IsNullOrEmpty(basepath))
			{
				if (!sftpClient.Exists(basepath))
				{
					sftpClient.CreateDirectory(basepath);
				}
			}
			// Ensure subfolder (if any) exists:
			folderPath = GetSubfolderPath(folderPath);
			if (!string.IsNullOrEmpty(folderPath))
			{
				if (!sftpClient.Exists(folderPath))
				{
					sftpClient.CreateDirectory(folderPath);
				}
			}

			// Set full destination file path (note that folderPath will have been set to a directly-usable value
			// by GetFileList, when it originally evaluated path) and handle existing file, if not overwriting:
			string destPath = $"{folderPath}{fileName}";
			if (preventOverwrite)
			{
				destPath = GetNextFilename(destPath);
			}

			// Open write stream to destination path and return:
			return new StreamPath
			{
				stream = sftpClient.OpenWrite(destPath),
				path = destPath
			};
		}
		/// <summary>
		/// Perform transfer of data from file at specified path to destination stream
		/// </summary>
		/// <remarks>Used only when this is "source" connection</remarks>
		public override async Task DoTransfer(string folderPath, string fileName, Stream writestream)
		{
			// Open read stream to source file (note that folderPath will have been set to a directly-usable
			// value by GetFileList, when it originally evaluated path):
			using var readstream = sftpClient.OpenRead($"{folderPath}{fileName}");

			if (config.PGP)
			{
				// Decrypt source stream contents into destination stream (base class must have initialized pgpKeyStream):
				await TaskUtilities.Pgp.Decrypt(PGPKeyStream, config.pgpPassphrase, readstream, writestream);
			}
			else
			{
				// Do direct copy of data from read stream to write:
				await readstream.CopyToAsync(writestream);
			}
		}
		#endregion

		#region Connection implementation - File management
		/// <summary>
		/// Rename specified file
		/// </summary>
		public override void RenameFile(string folderPath, string fileName, string newFileName, bool preventOverwrite)
		{
			folderPath = GetSubfolderPath(folderPath);
			sftpClient.RenameFile($"{folderPath}{fileName}", preventOverwrite ? GetNextFilename($"{folderPath}{newFileName}") : $"{folderPath}{newFileName}");
		}
		/// <summary>
		/// Delete specified file
		/// </summary>
		/// <remarks>Used only when this is "source" connection</remarks>
		public override void DeleteFile(string folderPath, string fileName)
		{
			//sftpClient.DeleteFile($"{folderPath}{fileName}");
			Console.WriteLine($"WOULD DELETE {folderPath}{fileName}"); // TODO: UNCOMMENT ACTUAL DELETE
		}
		#endregion

		#region Private methods
		/// <summary>
		/// Standardize folder path format
		/// </summary>
		private static string ScrubPath(string path)
		{
			// Clear whitespace, ensure all slashes are forward, replace null with string.Empty
			return string.IsNullOrWhiteSpace(path) ? string.Empty : path.Trim().Replace('\\', '/');
		}

		/// <summary>
		/// Build (and properly format) absolute path of specified subfolder
		/// </summary>
		private string GetSubfolderPath(string subfolder)
		{
			subfolder = ScrubPath(subfolder);

			// If no subfolder specified, just return basepath (guaranteed to be blank or end in "/")
			if (string.IsNullOrEmpty(subfolder))
			{
				return basepath;
			}
			// If subfolder indicates an absolute path or there is no basepath, just return subfolder (ensuring trailing slash):
			else if (subfolder.StartsWith('/') || string.IsNullOrEmpty(basepath))
			{
				return subfolder.EndsWith('/') ? subfolder : subfolder + "/";
			}
			// Otherwise, we have both a basepath and a non-absolute-path subfolder - combine and return (ensuring trailing slash):
			return subfolder.EndsWith('/') ? basepath + subfolder : basepath + subfolder + "/";
		}

		/// <summary>
		/// Check whether existing filename exists at specified path; if so, generate a
		/// unique filename by adding an integer suffix (incrementing until not found)
		/// </summary>
		private string GetNextFilename(string filePath, int maxDuplicates = 10)
		{
			if (sftpClient.Exists(filePath))
			{
				for (int i = 0; i < maxDuplicates; i++)
				{
					var testPath = $"{filePath}.{i}";
					if (!sftpClient.Exists(testPath))
					{
						return testPath;
					}
				}
				// If this point is reached, maximum duplicate files found - throw exception:
				throw new Exception($"Maximum number of duplicate files at path {filePath}");
			}
			else return filePath;
		}
		#endregion
	}
}

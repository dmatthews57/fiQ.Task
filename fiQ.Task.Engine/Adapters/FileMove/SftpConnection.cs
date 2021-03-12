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
		private Uri uri = null;
		private SftpClient sftpClient = null;
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
		public override void Connect()
		{
			// SftpClient will throw unfriendly error for this specific case; check first:
			if (string.IsNullOrEmpty(config.userID))
			{
				throw new ArgumentException("SFTP UserID required");
			}
			uri = new Uri(config.location);

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

			// If base path includes a subfolder, change directory now:
			var ap = uri.AbsolutePath;
			if (ap.StartsWith("/"))
			{
				// Remove leading slash so path is by default relative to home folder - URI can be configured with "//" in order
				// to force path from root folder, if required (i.e. sftp://host/relativetohome vs sftp://host//relativetoroot")
				ap = ap[1..];
			}
			if (!string.IsNullOrEmpty(ap))
			{
				sftpClient.ChangeDirectory(ap);
			}
		}
		/// <summary>
		/// Disconnection function - gracefully disconnect from server, if connected
		/// </summary>
		public override void Disconnect()
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
		public override HashSet<DownloadFile> GetFileList(List<SourceFilePath> paths)
		{
			var fileset = new HashSet<DownloadFile>();
			foreach (var path in paths)
			{
				// Construct Regex for filename filter (required), and for filename regex (optional):
				var fileFilterRegex = TaskUtilities.General.RegexFromFileFilter(path.FilenameFilter);
				var fileNameRegex = TaskUtilities.General.RegexIfPresent(path.FilenameRegex, RegexOptions.IgnoreCase);

				fileset.UnionWith(
					// Search current path if no subfolder specified, ignore leading "/" in subfolder, if present:
					sftpClient.ListDirectory(string.IsNullOrEmpty(path.FolderPath) ? "." : (path.FolderPath.StartsWith('/') ? path.FolderPath.Substring(1) : path.FolderPath))
						.Where(sf =>
							sf.IsDirectory == false
							&& fileFilterRegex.IsMatch(sf.Name)
							&& (fileNameRegex == null ? true : fileNameRegex.IsMatch(sf.Name))
						)
						.Select(sf => new DownloadFile(path)
						{
							fileName = sf.Name,
							lastWriteTime = sf.LastWriteTime,
							size = sf.Length
						})
				);
			}
			return fileset;
		}
		/// <summary>
		/// Open writable stream for specified destination file
		/// </summary>
		public override Stream GetWriteStream(string folderPath, string fileName, bool preventOverwrite)
		{
			string destPath = fileName;
			if (!string.IsNullOrEmpty(folderPath))
			{
				// If subfolder does not exist, create now:
				folderPath = ScrubPath(folderPath);
				if (!sftpClient.Exists(folderPath))
				{
					sftpClient.CreateDirectory(folderPath);
				}

				// Combine path with filename (adding trailing '/' to path, if required):
				destPath = $"{folderPath}{fileName}";
			}

			// Handle existing file, if not overwriting:
			if (preventOverwrite)
			{
				destPath = GetNextFilename(destPath);
			}

			// Open write stream to destination path and return:
			return sftpClient.OpenWrite(destPath);
		}
		/// <summary>
		/// Perform transfer of data from file at specified path to destination stream
		/// </summary>
		public override async Task DoTransfer(string folderPath, string fileName, Stream writestream)
		{
			// Open read stream to source file:
			using var readstream = sftpClient.OpenRead($"{ScrubPath(folderPath)}{fileName}");

			if (config.PGP)
			{
				// Open private key file and decrypt source stream contents into destination stream:
				using var privatekeystream = new FileStream(config.pgpKeyRing, FileMode.Open, FileAccess.Read, FileShare.Read);
				await TaskUtilities.Pgp.Decrypt(privatekeystream, config.pgpPassphrase, readstream, writestream);
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
		public override void RenameFile(string folderPath, string fileName, string newFileName)
		{
			folderPath = ScrubPath(folderPath);
			sftpClient.RenameFile($"{folderPath}{fileName}", $"{folderPath}{fileName}");
		}
		/// <summary>
		/// Delete specified file
		/// </summary>
		public override void DeleteFile(string folderPath, string fileName)
		{
			sftpClient.DeleteFile($"{ScrubPath(folderPath)}{fileName}");
		}
		#endregion

		#region Private methods
		/// <summary>
		/// Standardize subfolder path for use in other functions
		/// </summary>
		private static string ScrubPath(string path)
		{
			if (!string.IsNullOrEmpty(path))
			{
				path = path.Replace('\\', '/'); // Ensure all slashes are forward
				if (path.StartsWith('/')) // Ignore leading slash
				{
					path = path.Substring(1);
				}
				return path.EndsWith('/') ? path : $"{path}/"; // Ensure result has trailing slash
			}
			return path;
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

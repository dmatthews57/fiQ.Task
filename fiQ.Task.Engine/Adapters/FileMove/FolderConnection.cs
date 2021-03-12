using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace fiQ.TaskAdapters.FileMove
{
	class FolderConnection : Connection
	{
		#region Connection implementation - Connection management
		/// <summary>
		/// Connection function - currently unused (may perform impersonation in future, if required)
		/// </summary>
		public override void Connect()
		{
		}
		/// <summary>
		/// Disconnection function - currently unused (may clean up impersonation resources in future, if required)
		/// </summary>
		public override void Disconnect()
		{
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
				// If explicit filename regex provided in path, use it (otherwise create one from file filter,
				// to avoid false-positives on 8.3 version of filenames):
				var filenameRegex = TaskUtilities.General.RegexIfPresent(path.FilenameRegex, RegexOptions.IgnoreCase)
					?? TaskUtilities.General.RegexFromFileFilter(path.FilenameFilter);

				// Enumerate files in specified folder matching filter, apply regex and construct DownloadFiles:
				fileset.UnionWith(
					Directory.EnumerateFiles(string.IsNullOrEmpty(path.FolderPath) ? config.location : Path.Combine(config.location, path.FolderPath), path.FilenameFilter)
						.Where(fileName => filenameRegex.IsMatch(Path.GetFileName(fileName)))
						.Select(fileName => new FileInfo(fileName))
						.Select(fileInfo => new DownloadFile(path)
						{
							fileName = fileInfo.Name,
							lastWriteTime = fileInfo.LastWriteTime,
							size = fileInfo.Length
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
			// Ensure destination base folder exists:
			string destPath = config.location;
			if (!Directory.Exists(destPath))
			{
				Directory.CreateDirectory(destPath);
			}
			// If subfolder specified, add to destination path and ensure subfolder exists:
			if (!string.IsNullOrEmpty(folderPath))
			{
				destPath = Path.Combine(destPath, folderPath);
				if (!Directory.Exists(destPath))
				{
					Directory.CreateDirectory(destPath);
				}
			}

			// Add filename to destination path and handle existing file, if not overwriting:
			destPath = Path.Combine(destPath, fileName);
			if (preventOverwrite)
			{
				destPath = GetNextFilename(destPath);
			}

			// Return FileStream object writing to specified destination path:
			return new FileStream(destPath, FileMode.Create, FileAccess.Write, FileShare.None);
		}
		/// <summary>
		/// Perform transfer of data from file at specified path to destination stream
		/// </summary>
		public override async Task DoTransfer(string folderPath, string fileName, Stream writestream)
		{
			// Open read stream on source file:
			using var readstream = new FileStream(string.IsNullOrEmpty(folderPath) ? Path.Combine(config.location, fileName) : Path.Combine(config.location, folderPath, fileName),
				FileMode.Open, FileAccess.ReadWrite, FileShare.None);

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
			// Combine base location and subfolder path, handle duplicate filenames, then rename:
			folderPath = string.IsNullOrEmpty(folderPath) ? config.location : Path.Combine(config.location, folderPath);
			string destFilePath = GetNextFilename(Path.Combine(folderPath, newFileName));
			File.Move(Path.Combine(folderPath, fileName), destFilePath);
		}
		/// <summary>
		/// Delete specified file
		/// </summary>
		public override void DeleteFile(string folderPath, string fileName)
		{
			File.Delete(string.IsNullOrEmpty(folderPath) ? Path.Combine(config.location, fileName) : Path.Combine(config.location, folderPath, fileName));
		}
		#endregion

		#region Private methods
		/// <summary>
		/// Check whether existing filename exists at specified path; if so, generate a
		/// unique filename by adding an integer suffix (incrementing until not found)
		/// </summary>
		private static string GetNextFilename(string filePath, int maxDuplicates = 10)
		{
			if (File.Exists(filePath))
			{
				for (int i = 0; i < maxDuplicates; i++)
				{
					var testPath = $"{filePath}.{i}";
					if (!File.Exists(testPath))
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

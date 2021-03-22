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
		#region Fields and constructor
		public FolderConnection(ConnectionConfig _config) : base(_config)
		{
		}
		#endregion

		#region Connection implementation - Connection management
		/// <summary>
		/// Connection function - currently unused (may perform impersonation in future, if required)
		/// </summary>
		protected override void DoConnect()
		{
		}
		/// <summary>
		/// Disconnection function - currently unused (may clean up impersonation resources in future, if required)
		/// </summary>
		protected override void DoDisconnect()
		{
		}
		#endregion

		#region Connection implementation - File transfer (source mode)
		/// <summary>
		/// Retrieve listing of downloadable files
		/// </summary>
		/// <param name="paths">Collection of SourceFilePaths to search</param>
		/// <returns>HashSet of downloadable files in source paths, matching source patterns</returns>
		public override HashSet<DownloadFile> GetFileList(List<SourceFilePath> paths)
		{
			var fileset = new HashSet<DownloadFile>();
			foreach (var path in paths)
			{
				// Construct Regex for filename filter (required, avoid false-positives on 8.3 version of filename), and for filename regex (optional):
				var fileFilterRegex = TaskUtilities.General.RegexFromFileFilter(path.FilenameFilter);
				var fileNameRegex = TaskUtilities.General.RegexIfPresent(path.FilenameRegex, RegexOptions.IgnoreCase);

				// Enumerate files in specified folder matching filter, apply regex andex construct DownloadFiles:
				string subfolderPath = string.IsNullOrEmpty(path.FolderPath) ? config.location : TaskUtilities.General.PathCombine(config.location, path.FolderPath);
				fileset.UnionWith(
					Directory.EnumerateFiles(subfolderPath, path.FilenameFilter)
						.Where(fileName =>
							fileFilterRegex.IsMatch(Path.GetFileName(fileName))
							&& (fileNameRegex == null ? true : fileNameRegex.IsMatch(fileName))
						)
						.Select(fileName => new FileInfo(fileName))
						.Select(fileInfo => new DownloadFile
						{
							// Note that full source path is replacing configured FolderPath value here, so later
							// operations can just use it directly without re-evaluating:
							fileFolder = subfolderPath,
							fileName = fileInfo.Name,
							lastWriteTime = fileInfo.LastWriteTime,
							size = fileInfo.Length,
							DestinationSubfolder = path.DestinationSubfolder
						})
				);
			}
			return fileset;
		}
		/// <summary>
		/// Perform transfer of data from file at specified path to destination stream
		/// </summary>
		/// <remarks>Used only when this is "source" connection</remarks>
		public override async Task DoTransfer(string folderPath, string fileName, Stream writestream)
		{
			// Open read stream on source file (note that folderPath will have been set to a directly-usable
			// value by GetFileList, when it originally evaluated path):
			using var readstream = new FileStream(TaskUtilities.General.PathCombine(folderPath, fileName), FileMode.Open, FileAccess.ReadWrite, FileShare.None);

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
		/// <summary>
		/// Delete specified file
		/// </summary>
		public override void DeleteFile(string folderPath, string fileName)
		{
			// Note that folderPath will have been set to a directly-usable value by GetFileList:
			File.Delete(TaskUtilities.General.PathCombine(folderPath, fileName));
		}
		public override void SourceRenameFile(string folderPath, string fileName, string newFileName, bool preventOverwrite)
		{
			// Note that folderPath will have been set to a directly-usable value by GetFileList:
			string destFilePath = TaskUtilities.General.PathCombine(folderPath, newFileName);
			File.Move(TaskUtilities.General.PathCombine(folderPath, fileName), preventOverwrite ? GetNextFilename(destFilePath) : destFilePath);
		}
		#endregion

		#region Connection implementation - File transfer (destination mode)
		/// <summary>
		/// Open writable stream for specified destination file
		/// </summary>
		public override StreamPath GetWriteStream(string folderPath, string fileName, bool preventOverwrite)
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
				destPath = TaskUtilities.General.PathCombine(destPath, folderPath);
				if (!Directory.Exists(destPath))
				{
					Directory.CreateDirectory(destPath);
				}
			}

			// Add filename to destination path and handle existing file, if not overwriting:
			destPath = TaskUtilities.General.PathCombine(destPath, fileName);
			if (preventOverwrite)
			{
				destPath = GetNextFilename(destPath);
			}

			// Return StreamPath containing FileStream object writing to specified destination path:
			return new StreamPath
			{
				stream = new FileStream(destPath, FileMode.Create, FileAccess.Write, FileShare.None),
				path = destPath
			};
		}
		/// <summary>
		/// Perform cleanup/finalization steps on StreamPath (not required)
		/// </summary>
		public override async Task FinalizeWrite(StreamPath streampath)
		{
		}
		/// <summary>
		/// Perform deferred rename of file at destination (no action required, ignore)
		/// </summary>
		public override void DestRenameFile(string folderPath, string fileName, string newFileName, bool preventOverwrite)
		{
			// Combine base location and subfolder path, handle duplicate filenames if needed, then rename:
			folderPath = string.IsNullOrEmpty(folderPath) ? config.location : TaskUtilities.General.PathCombine(config.location, folderPath);
			string destFilePath = TaskUtilities.General.PathCombine(folderPath, newFileName);
			File.Move(TaskUtilities.General.PathCombine(folderPath, fileName), preventOverwrite ? GetNextFilename(destFilePath) : destFilePath);
		}
		#endregion

		#region Connection implementation - Simple method file transfer
		/// <summary>
		/// Simple file copy operation is supported only if this object is not using PGP and source
		/// connection is ALSO a FolderConnection which is not using PGP
		/// </summary>
		public override bool SupportsSimpleCopy(Connection sourceConnection)
		{
			return !config.PGP && sourceConnection is FolderConnection fc && !fc.config.PGP;
		}
		/// <summary>
		/// Perform simple file copy (no encryption) from a FolderConnection source
		/// </summary>
		/// <returns>Path to destination file</returns>
		public override string DoSimpleCopy(Connection sourceConnection, string sourceFolderPath, string sourceFileName,
			string destFolderPath, string destFileName, bool preventOverwrite)
		{
			if (sourceConnection is not FolderConnection sourcefc)
			{
				throw new InvalidOperationException("Source connection type not supported for simple copy");
			}
			else if (sourcefc.config.PGP)
			{
				throw new InvalidOperationException("Source connection using PGP, not supported for simple copy");
			}

			// Start with destination base folder and ensure it exists:
			string destPath = config.location;
			if (!Directory.Exists(destPath))
			{
				Directory.CreateDirectory(destPath);
			}
			// If destination subfolder specified, add to destination path and ensure subfolder exists:
			if (!string.IsNullOrEmpty(destFolderPath))
			{
				destPath = TaskUtilities.General.PathCombine(destPath, destFolderPath);
				if (!Directory.Exists(destPath))
				{
					Directory.CreateDirectory(destPath);
				}
			}
			// Finally, add filename to destination path and handle existing file (if not overwriting):
			destPath = TaskUtilities.General.PathCombine(destPath, destFileName);
			if (preventOverwrite)
			{
				destPath = GetNextFilename(destPath);
			}

			// Perform simple file copy operation (note source path will have been set to directly-usable value
			// by source connection's GetFileList, when it originally evaluated path):
			File.Copy(TaskUtilities.General.PathCombine(sourceFolderPath, sourceFileName), destPath, !preventOverwrite);

			// Return final destination path:
			return destPath;
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

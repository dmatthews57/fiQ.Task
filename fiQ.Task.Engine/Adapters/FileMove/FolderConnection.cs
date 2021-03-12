using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

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

		#region Connection implementation - Files
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
		#endregion
	}
}

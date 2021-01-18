using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using fiQ.Task.Models;
using fiQ.Task.Utilities;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.Task.Adapters
{
	/// <summary>
	/// TaskAdapter to check for presence of file(s) matching a specified pattern
	/// </summary>
	/// <remarks>
	/// This adapter is really only useful when run as part of a batch, with HaltOnError set to true (to abort if expected file does not exist)
	/// </remarks>
	public class FileCheckAdapter : TaskAdapter
	{
		#region Fields and constructors
		public FileCheckAdapter(IConfiguration _config, ILogger<FileCheckAdapter> _logger, string taskName = null)
			: base(_config, _logger, taskName) { }
		#endregion

		/// <summary>
		/// Check whether specified file(s) exist and set result accordingly
		/// </summary>
		public override async Task<TaskResult> ExecuteTask(TaskParameters parameters)
		{
			var result = new TaskResult();
			try
			{
				#region Retrieve task parameters
				string sourceFolder = parameters.GetString("SourceFolder", TaskUtilities.REGEX_DIRPATH, DateTime.Now);
				string filenameFilter = parameters.GetString("FilenameFilter");
				if (string.IsNullOrEmpty(sourceFolder) || string.IsNullOrEmpty(filenameFilter))
				{
					throw new ArgumentException("Missing SourceFolder and/or FilenameFilter");
				}
				string filenameRegex = parameters.GetString("FilenameRegex");
				bool recurseFolders = parameters.GetBool("RecurseFolders");

				// Create regex object from custom string, if provided (and from file filter, otherwise - this
				// check is performed to avoid false-positives on 8.3 version of filenames):
				var rFilenameRegex = string.IsNullOrEmpty(filenameRegex) ? TaskUtilities.RegexFromFileFilter(filenameFilter)
					: new Regex(filenameRegex, RegexOptions.IgnoreCase);
				#endregion

				// Set successful result if any files found in specified folder matching filter and regex:
				result.Success = Directory.EnumerateFiles(sourceFolder, filenameFilter, recurseFolders ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly)
					.Select(Path.GetFileName)
					.Where(fileName => rFilenameRegex.IsMatch(fileName))
					.Any();
			}
			catch (Exception ex)
			{
				logger.LogError(ex, "File checking failed");
				result.AddException(ex);
			}
			return result;
		}
	}
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using fiQ.TaskAdapters.FileMove;
using fiQ.TaskModels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.TaskAdapters
{
	public class FileMoveAdapter : TaskAdapter
	{
		#region Fields and constructors
		private static readonly JsonSerializerOptions downloadListFileFormat = new JsonSerializerOptions { WriteIndented = true };
		private ConnectionConfig sourceConnectionConfig = null;
		private ConnectionConfig destConnectionConfig = null;
		private bool copyFileOnly = false;				// If true, file will be copied instead of moved
		private string fileRenameRegex = null;			// Regex for renaming file during transfer
		private string fileRenameReplacement = null;	// Replacement value for renaming file during transfer
		private bool fileRenameDefer = false;			// If true, file will be renamed after transfer complete
		private string sourceFileRenameRegex = null;	// Regex for renaming source file after transfer (if copyFileOnly)
		private string sourceFileRenameReplacement = null;  // Replacement value for renaming source file after transfer complete (if copyFileOnly)
		private bool suppressErrors = false;			// If true, errors will not be factored into success/failure of this task

		public FileMoveAdapter(IConfiguration _config, ILogger<FileMoveAdapter> _logger, string taskName = null)
			: base(_config, _logger, taskName) {}
		#endregion

		/// <summary>
		/// TODO: SUMMARY
		/// </summary>
		public override async Task<TaskResult> ExecuteTask(TaskParameters parameters)
		{
			var result = new TaskResult();
			var dateTimeNow = DateTime.Now; // Use single value throughout for consistency in macro replacements
			Stream downloadListFile = null;
			try
			{
				#region Retrieve task parameters
				// Load source connection configuration and validate:
				sourceConnectionConfig = new ConnectionConfig(parameters, true);
				if (sourceConnectionConfig.Invalid)
				{
					throw new ArgumentException("Source configuration invalid");
				}
				// Load destination connection configuration and validate:
				destConnectionConfig = new ConnectionConfig(parameters, false);
				if (destConnectionConfig.Invalid)
				{
					throw new ArgumentException("Destination configuration invalid");
				}

				// Build collection of source file path/filter combinations starting with standard single values in parameter collection:
				var sourceFilePaths = new List<SourceFilePath>
				{
					new SourceFilePath
					{
						FolderPath = parameters.GetString("SourceFolderPath", null, dateTimeNow),
						FilenameFilter = parameters.GetString("SourceFilenameFilter", null, dateTimeNow),
						FilenameRegex = parameters.GetString("SourceFilenameRegex")
					}
				};
				// Add path collection from separate configuration section, if present:
				var sourcePathsConfig = parameters.Configuration.GetSection("SourceFilePaths");
				if (sourcePathsConfig.Exists())
				{
					sourceFilePaths.AddRange(sourcePathsConfig.Get<IEnumerable<SourceFilePath>>());
				}
				// Remove invalid entries and ensure there are source paths present:
				sourceFilePaths.RemoveAll(path => path.Invalid());
				if (sourceFilePaths.Count == 0)
				{
					throw new ArgumentException("No valid source file paths found");
				}

				// Read optional parameters:
				copyFileOnly = parameters.GetBool("CopyFileOnly");
				fileRenameRegex = parameters.GetString("FileRenameRegex");
				if (!string.IsNullOrEmpty(fileRenameRegex))
				{
					fileRenameReplacement = parameters.GetString("FileRenameReplacement", null, dateTimeNow);
					fileRenameDefer = parameters.GetBool("FileRenameDefer");
				}
				sourceFileRenameRegex = parameters.GetString("SourceFileRenameRegex");
				if (!string.IsNullOrEmpty(sourceFileRenameRegex))
				{
					sourceFileRenameReplacement = parameters.GetString("SourceFileRenameReplacement", null, dateTimeNow);
				}
				#endregion

				#region If download listing is configured, open and read
				DownloadFileList downloadFileList = null;
				string downloadListFilename = parameters.GetString("DownloadListFilename");
				if (!string.IsNullOrEmpty(downloadListFilename))
				{
					if (File.Exists(downloadListFilename))
					{
						// Open file (note this FileStream will be kept open until finally block below) and attempt to
						// deserialize file listing from its contents:
						downloadListFile = new FileStream(downloadListFilename, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
						downloadFileList = downloadListFile.Length > 0 ? await JsonSerializer.DeserializeAsync<DownloadFileList>(downloadListFile) : new DownloadFileList();

						// If there are any files in listing, check whether we need to prune the list by download age:
						if (downloadFileList?.downloadFiles?.Count > 0)
						{
							if (downloadFileList.PruneList(parameters.Get<TimeSpan>("DownloadListMaxAge", TimeSpan.TryParse)))
							{
								// File listing was modified - truncate file and re-write contents:
								downloadListFile.SetLength(0);
								await JsonSerializer.SerializeAsync(downloadListFile, downloadFileList, downloadListFileFormat);
							}
						}
					}
					else
					{
						// Create file, leave stream open for use below:
						downloadListFile = new FileStream(downloadListFilename, FileMode.Create, FileAccess.Write, FileShare.None);
						downloadFileList = new DownloadFileList();
					}
				}
				#endregion

				// Create source connection, connect and retrieve file listing
				using (var sourceConnection = Connection.CreateInstance(sourceConnectionConfig))
				{
					sourceConnection.Connect();
					var fileList = sourceConnection.GetFileList(sourceFilePaths);
					if (fileList.Any() && downloadFileList?.downloadFiles?.Count > 0)
					{
						// Remove any files already present in downloaded list from current set:
						fileList.ExceptWith(downloadFileList.downloadFiles);
					}

					if (fileList.Any())
					{
						#region Open destination connection and transfer files
						using var destConnection = Connection.CreateInstance(destConnectionConfig);
						destConnection.Connect();
						//foreach (var file in fileList)
						//{
							// TODO: CREATE DEST, DO COPIES...
						//}
						if (downloadFileList != null)
						{
							downloadFileList.downloadFiles.UnionWith(fileList);
							downloadListFile.SetLength(0);
							await JsonSerializer.SerializeAsync(downloadListFile, downloadFileList, downloadListFileFormat);
						}
						#endregion
					}
				}

				// If this point is reached with no exceptions thrown, set success:
				result.Success = true;
			}
			catch (Exception ex)
			{
				if (ex is AggregateException ae) // Exception caught from async task; simplify if possible
				{
					ex = TaskUtilities.General.SimplifyAggregateException(ae);
				}
				logger.LogError(ex, "Move process failed");

				// Error has been logged; if we are suppressing errors consider this successful, otherwise add
				// exception to return collection for caller to handle:
				if (suppressErrors)
				{
					result.Success = true;
				}
				else
				{
					result.AddException(ex);
				}
			}
			finally
			{
				if (downloadListFile != null)
				{
					await downloadListFile.DisposeAsync();
					downloadListFile = null;
				}
			}
			return result;
		}
	}
}

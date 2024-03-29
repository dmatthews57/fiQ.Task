﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
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
		public FileMoveAdapter(IServiceProvider _isp, IConfiguration _config, ILogger<FileMoveAdapter> _logger, string taskName = null)
			: base(_isp, _config, _logger, taskName) {}
		#endregion

		/// <summary>
		/// Move or copy all files matching a pattern from a source location to a destination location, optionally
		/// encrypting/decrypting/renaming in the process
		/// </summary>
		public override async Task<TaskResult> ExecuteTask(TaskParameters parameters)
		{
			var result = new TaskResult();
			var dateTimeNow = DateTime.Now; // Use single value throughout for consistency in macro replacements
			bool suppressErrors = false; // If true, errors will not be factored into success/failure of this task
			Stream downloadListFile = null; // If used, download listing file will be held open until finally block
			try
			{
				#region Retrieve task parameters
				// Load source connection configuration and validate:
				var sourceConnectionConfig = new ConnectionConfig(parameters, true);
				if (sourceConnectionConfig.Invalid)
				{
					throw new ArgumentException("Source configuration invalid");
				}
				// Load destination connection configuration and validate:
				var destConnectionConfig = new ConnectionConfig(parameters, false);
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

				// Read optional parameters - behavior:
				suppressErrors = parameters.GetBool("SuppressErrors");
				var maxAge = parameters.Get<TimeSpan>("MaxAge", TimeSpan.TryParse);
				var preventOverwrite = parameters.GetBool("PreventOverwrite");
				var copyFileOnly = parameters.GetBool("CopyFileOnly");

				// Read optional parameters - rename file at destination:
				var fileRenameRegex = TaskUtilities.General.RegexIfPresent(parameters.GetString("FileRenameRegex"), RegexOptions.IgnoreCase);
				string fileRenameReplacement = null;
				bool fileRenameDefer = false; // If true, file will be renamed after transfer complete
				if (fileRenameRegex != null)
				{
					fileRenameDefer = parameters.GetBool("FileRenameDefer");
					if (fileRenameDefer && preventOverwrite)
					{
						// Deferred rename occurs at remote site; with preventOverwrite on, original file may be uploaded under another name
						// by destination object (meaning rename step would fail), and final filename will not be checked in advance:
						throw new ArgumentException("PreventOverwrite and FileRenameDefer options cannot be used together");
					}
					fileRenameReplacement = parameters.GetString("FileRenameReplacement", null, dateTimeNow) ?? string.Empty;
				}

				// Read optional parameters - rename source file after copy:
				var sourceFileRenameRegex = TaskUtilities.General.RegexIfPresent(parameters.GetString("SourceFileRenameRegex"), RegexOptions.IgnoreCase);
				string sourceFileRenameReplacement = null;
				if (sourceFileRenameRegex != null)
				{
					if (!copyFileOnly)
					{
						// If CopyFileOnly is not set, source file will be deleted (and thus cannot be renamed); raise error to ensure that
						// unintended behavior does not occur (allow opportunity to correct configuration):
						throw new ArgumentException("SourceFileRenameRegex cannot be used unless CopyFileOnly is set");
					}
					sourceFileRenameReplacement = parameters.GetString("SourceFileRenameReplacement", null, dateTimeNow) ?? string.Empty;
				}
				#endregion

				#region If file download history listing is configured, open file and read
				DownloadFileList downloadFileList = null;
				string downloadListFilename = parameters.GetString("DownloadListFilename");
				if (!string.IsNullOrEmpty(downloadListFilename))
				{
					if (File.Exists(downloadListFilename))
					{
						// Open file (note this FileStream will be kept open until finally block below) and attempt to
						// deserialize file listing from its contents (if any):
						downloadListFile = new FileStream(downloadListFilename, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
						downloadFileList = downloadListFile.Length > 0 ? await JsonSerializer.DeserializeAsync<DownloadFileList>(downloadListFile) : new DownloadFileList();

						// If there are any files in listing, prune list by download age:
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
						// Create new empty file (leave stream open for use below) and new empty listing:
						downloadListFile = new FileStream(downloadListFilename, FileMode.Create, FileAccess.Write, FileShare.None);
						downloadFileList = new DownloadFileList();
					}
				}
				#endregion

				// Create source connection, connect and retrieve file listing
				using (var sourceConnection = Connection.CreateInstance(isp, sourceConnectionConfig))
				{
					sourceConnection.Connect();
					var fileList = sourceConnection.GetFileList(sourceFilePaths);
					if (fileList.Any() && downloadFileList?.downloadFiles?.Count > 0)
					{
						// Remove any files already present in downloaded list from current set:
						fileList.ExceptWith(downloadFileList.downloadFiles);
					}
					if (fileList.Any() && maxAge != null)
					{
						// Remove any files older than maxAge (ignoring sign - allow age to be specified either way):
						var minLastWriteTime = dateTimeNow.Add((TimeSpan)(maxAge?.TotalMilliseconds > 0 ? maxAge?.Negate() : maxAge));
						fileList.RemoveWhere(file => file.lastWriteTime < minLastWriteTime);
					}

					if (fileList.Any())
					{
						#region Open destination connection and transfer files
						using var destConnection = Connection.CreateInstance(isp, destConnectionConfig);
						destConnection.Connect();
						foreach (var file in fileList)
						{
							using var logscope = new TaskUtilities.LogScopeHelper(logger, new Dictionary<string, object>()
							{
								["FileFolder"] = file.fileFolder,
								["FileName"] = file.fileName,
								["SourcePGP"] = sourceConnectionConfig.PGP,
								["DestPGP"] = destConnectionConfig.PGP
							});
							try
							{
								#region Perform file transfer
								// Apply file rename, if appropriate:
								string destFileName = (fileRenameRegex == null || fileRenameDefer) ? file.fileName : fileRenameRegex.Replace(file.fileName, fileRenameReplacement);
								logger.LogDebug($"File will be transferred to {(string.IsNullOrEmpty(file.DestinationSubfolder) ? destFileName : TaskUtilities.General.PathCombine(file.DestinationSubfolder, destFileName))}");

								// If simple copy method supported by this source/dest combination, perform now:
								if (destConnection.SupportsSimpleCopy(sourceConnection))
								{
									string destPath = destConnection.DoSimpleCopy(sourceConnection, file.fileFolder, file.fileName, file.DestinationSubfolder, destFileName, preventOverwrite);
									logscope.AddToState("DestFilePath", destPath);
									logger.LogInformation("File transferred (using simple copy)");
								}
								else // Non-folder source/destination or PGP encryption/decryption required
								{
									// Request write stream from destination object:
									using var deststream = destConnection.GetWriteStream(file.DestinationSubfolder, destFileName, preventOverwrite);
									logscope.AddToState("DestFilePath", deststream.path);
									if (destConnectionConfig.PGP)
									{
										// Destination requires PGP encryption - create encrypting stream around destination stream:
										using var encstream = destConnectionConfig.pgpRawFormat ?
											await TaskUtilities.Pgp.GetEncryptionStreamRaw(destConnection.PGPKeyStream, destConnectionConfig.pgpUserID, deststream.stream)
											: await TaskUtilities.Pgp.GetEncryptionStream(destConnection.PGPKeyStream, destConnectionConfig.pgpUserID, deststream.stream);

										// Order source connection to write data from specified file into encrypting stream:
										await sourceConnection.DoTransfer(file.fileFolder, file.fileName, encstream.GetStream());
									}
									else
									{
										// No encryption required - order source connection to write data from specified file into destination stream:
										await sourceConnection.DoTransfer(file.fileFolder, file.fileName, deststream.stream);
									}
									await destConnection.FinalizeWrite(deststream);
									logger.LogInformation("File transferred");
								}

								// If file rename deferred, perform now:
								if (fileRenameDefer)
								{
									var renameFile = fileRenameRegex.Replace(destFileName, fileRenameReplacement);
									logscope.AddToState("DestFileRenamePath", renameFile);
									destConnection.DestRenameFile(file.DestinationSubfolder, destFileName, renameFile, preventOverwrite);
									logger.LogInformation("File renamed at destination", renameFile);
								}
								#endregion

								#region Delete or rename source file
								if (copyFileOnly)
								{
									if (sourceFileRenameRegex != null)
									{
										var renameFile = sourceFileRenameRegex.Replace(file.fileName, sourceFileRenameReplacement);
										sourceConnection.SourceRenameFile(file.fileFolder, file.fileName, renameFile, preventOverwrite);
										logger.LogDebug($"Source file renamed to {renameFile}");
									}
								}
								else
								{
									sourceConnection.DeleteFile(file.fileFolder, file.fileName);
									logger.LogDebug("Source file deleted");
								}
								#endregion
							}
							catch (Exception ex)
							{
								logger.LogError(ex, "Error downloading file");
								if (!suppressErrors)
								{
									result.AddException(new Exception($"Error downloading file {TaskUtilities.General.PathCombine(file.fileFolder, file.fileName)}", ex));
								}
								continue;
							}

							#region Update file download list, if required
							if (downloadFileList != null)
							{
								try
								{
									// Clear file contents and re-serialize updated file listing (performed after each file transfer rather than
									// at end of job to ensure that a scenario where some files are downloaded and then the job/task is halted
									// or killed does not result in duplicate files being downloaded next run)
									downloadFileList.downloadFiles.Add(file);
									downloadListFile.SetLength(0);
									await JsonSerializer.SerializeAsync(downloadListFile, downloadFileList, downloadListFileFormat);
								}
								catch (Exception ex)
								{
									logger.LogError(ex, "Error updating file download listing");
									result.AddException(new Exception("Error updating download listing", ex));
								}
							}
							#endregion
						}
						destConnection.Disconnect();
						#endregion
					}
					sourceConnection.Disconnect();
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

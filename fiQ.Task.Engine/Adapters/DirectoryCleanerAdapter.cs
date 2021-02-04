using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using fiQ.TaskModels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.TaskAdapters
{
	public class DirectoryCleanerAdapter : TaskAdapter
	{
		#region Fields and constructors
		public DirectoryCleanerAdapter(IConfiguration _config, ILogger<DirectoryCleanerAdapter> _logger, string taskName = null)
			: base(_config, _logger, taskName) { }
		#endregion

		/// <summary>
		/// Iterate through a specified folder (and optionally, subfolders), cleaning up files over a specified age
		/// (optionally archiving them to zip file, first)
		/// </summary>
		public override async Task<TaskResult> ExecuteTask(TaskParameters parameters)
		{
			var result = new TaskResult();
			try
			{
				#region Retrieve task parameters
				// Retrieve and validate required parameters first:
				string sourceFolder = parameters.GetString("SourceFolder", TaskUtilities.General.REGEX_DIRPATH, DateTime.Now);
				string filenameFilter = parameters.GetString("FilenameFilter");
				var maxAge = parameters.Get<TimeSpan>("maxAge", TimeSpan.TryParse);
				if (string.IsNullOrEmpty(sourceFolder) || string.IsNullOrEmpty(filenameFilter) || maxAge == null)
				{
					throw new ArgumentException("Missing or invalid: one or more of SourceFolder/FilenameFilter/MaxAge");
				}
				// Ignore sign on TimeSpan value (allow time to be specified either way):
				var minLastWriteTime = DateTime.Now.Add((TimeSpan)(maxAge?.TotalMilliseconds > 0 ? maxAge?.Negate() : maxAge));

				// Retrieve optional parameters (general):
				string filenameRegex = parameters.GetString("FilenameRegex");
				bool recurseFolders = parameters.GetBool("RecurseFolders");

				// Retrieve optional archive parameters (indicating files should be zipped up prior to deletion); we will
				// validate ArchiveFolder value here rather than as part of GetString, to ensure that if it is invalid the
				// process is aborted rather than silently deleting files that should be archived:
				string archiveFolder = parameters.GetString("ArchiveFolder");
				if (!string.IsNullOrEmpty(archiveFolder))
				{
					if (!TaskUtilities.General.REGEX_DIRPATH.IsMatch(archiveFolder))
					{
						throw new ArgumentException("Invalid ArchiveFolder specified");
					}
				}
				string archiveSubfolder = string.IsNullOrEmpty(archiveFolder) ? null : parameters.GetString("ArchiveSubfolder");
				string archiveRenameRegex = string.IsNullOrEmpty(archiveFolder) ? null : parameters.GetString("ArchiveRenameRegex");
				string archiveRenameReplacement = string.IsNullOrEmpty(archiveRenameRegex) ? null : parameters.GetString("ArchiveRenameReplacement");
				var rArchiveRenameRegex = string.IsNullOrEmpty(archiveRenameReplacement) ? null : new Regex(archiveRenameRegex, RegexOptions.IgnoreCase);

				// Ensure sourceFolder ends with trailing slash, for proper relative paths in recursive folders:
				if (!sourceFolder.EndsWith(@"\"))
				{
					sourceFolder += @"\";
				}

				// Create regex object from custom string, if provided (and from file filter, otherwise - this
				// check is performed to avoid false-positives on 8.3 version of filenames):
				var rFilenameRegex = string.IsNullOrEmpty(filenameRegex) ? TaskUtilities.General.RegexFromFileFilter(filenameFilter)
					: new Regex(filenameRegex, RegexOptions.IgnoreCase);
				#endregion

				// Build listing of all files in source folder whose last write time is older than minimum value:
				var fileList = Directory.EnumerateFiles(sourceFolder, filenameFilter, recurseFolders ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly)
					.Where(fileName => rFilenameRegex.IsMatch(Path.GetFileName(fileName)))
					.Select(fileName => new { FileName = fileName, LastWriteTime = File.GetLastWriteTime(fileName) })
					.Where(file => file.LastWriteTime < minLastWriteTime);

				#region Handle all matching files
				if (fileList.Any())
				{
					// Set up shared objects for use throughout remainder of process:
					using var md5prov = IncrementalHash.CreateHash(HashAlgorithmName.MD5);

					foreach (var file in fileList)
					{
						try
						{
							// Check for an ArchiveFolder value (if present, we need to zip up file prior to deleting):
							if (!string.IsNullOrEmpty(archiveFolder))
							{
								#region Add file to zip archive
								if (file.FileName.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
								{
									// Do not add zip files to zip archive (zip archives may be created within the folder
									// structure we are currently cleaning, and we do not want to add zip to itself):
									continue;
								}

								// Apply date/time values to archive folder name, ensure resulting folder exists:
								string archiveFilePath = TaskUtilities.General.ApplyDateMacros(archiveFolder, file.LastWriteTime);
								if (!Directory.Exists(archiveFilePath))
								{
									Directory.CreateDirectory(archiveFilePath);
								}

								// If regular expression/replacement not provided, append simple archive filename in date-based format:
								if (string.IsNullOrEmpty(archiveRenameReplacement))
								{
									archiveFilePath = Path.Combine(archiveFilePath, $"{file.LastWriteTime:yyyy-MM}.zip");
								}
								// Otherwise append archive filename constructed using regex/replacement:
								else
								{
									archiveFilePath = Path.Combine(archiveFilePath,
										$"{rArchiveRenameRegex.Replace(Path.GetFileNameWithoutExtension(file.FileName), TaskUtilities.General.ApplyDateMacros(archiveRenameReplacement, file.LastWriteTime))}.zip");
								}

								// Open/create resulting zip archive:
								using (var archive = ZipFile.Open(archiveFilePath, ZipArchiveMode.Update))
								{
									// Determine path within zip under which this file will be placed: start with path
									// relative to our working folder (in case we are recursing subfolders), prefixed
									// with explicit subfolder name, if configured:
									string relativePath = Uri.UnescapeDataString(new Uri(sourceFolder).MakeRelativeUri(new Uri(file.FileName)).ToString())
										.Replace(Path.AltDirectorySeparatorChar, Path.DirectorySeparatorChar);
									if (!string.IsNullOrEmpty(archiveSubfolder))
									{
										relativePath = Path.Combine(TaskUtilities.General.ApplyDateMacros(archiveSubfolder, file.LastWriteTime), relativePath);
									}

									// Check whether this path already exists within this zip archive:
									var existingEntry = archive.GetEntry(relativePath);
									if (existingEntry != null)
									{
										// Existing entry found - we only want to add this file again if the contents are not equal to the
										// entry already in the archive, so calculate and compare file hashes:
										byte[] hashInZip = null;
										using (var zipStream = existingEntry.Open())
										{
											hashInZip = await TaskUtilities.Streams.GetStreamHash(zipStream, md5prov);
										}
										using (var fileStream = new FileStream(file.FileName, FileMode.Open, FileAccess.Read, FileShare.None))
										{
											// If zip hash sequence is NOT equal to the hash of the source file, clear existing entry
											// to indicate that the file SHOULD be added (CreateEntryFromFile allows duplicates):
											if (!hashInZip.SequenceEqual(await TaskUtilities.Streams.GetStreamHash(fileStream, md5prov)))
											{
												existingEntry = null;
											}
										}
									}

									// If there is no existing entry or entry was cleared above due to hash check, add to archive:
									if (existingEntry == null)
									{
										archive.CreateEntryFromFile(file.FileName, relativePath);
										logger.LogDebug($"File {file.FileName} added to archive {archiveFilePath}");
									}
								}
								#endregion
							}

							// If this point is reached, file can be deleted:
							File.Delete(file.FileName);
							logger.LogInformation($"File {file.FileName} deleted");
						}
						catch (Exception ex)
						{
							// Exception cleaning individual file should not leak out of overall task; just log (simplify if necessary):
							logger.LogWarning(ex is AggregateException ae ? TaskUtilities.General.SimplifyAggregateException(ae) : ex,
								$"Error cleaning file {file.FileName}");
						}
					}
				}
				#endregion

				// If this point is reached with no uncaught exceptions, return success
				result.Success = true;
			}
			catch (Exception ex)
			{
				logger.LogError(ex, "Directory cleaning failed");
				result.AddException(ex);
			}
			return result;
		}
	}
}

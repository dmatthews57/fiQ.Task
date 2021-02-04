using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using fiQ.TaskModels;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.TaskAdapters
{
	public class FileImportAdapter : TaskAdapter
	{
		#region Fields and constructors
		private IMemoryCache memorycache;   // Injected by constructor, used to cache SQL stored procedure parameters
		private IncrementalHash md5hash = null;
		private bool disposed = false;

		private string importProcedureName = null;		// Stored procedure for actual data import
		private int? importProcedureTimeout = null;		// Optional timeout (in seconds) for main import procedure call
		private string importPreProcessorName = null;	// Optional stored procedure for pre-processing file
		private int? importPreProcessorTimeout = null;	// Optional timeout (in seconds) for pre-processor call
		private string importPostProcessorName = null;	// Optional stored procedure for post-processing file
		private int? importPostProcessorTimeout = null;	// Optional timeout (in seconds) for post-processor call
		private string importPGPPrivateKeyRing = null;	// If present, data input will be PGP-decrypted with a key from the specified ring
		private string importPGPPassphrase = null;		// If PGP private keyring specified, passphrase to access private key
		private bool defaultNulls = false;				// If true, explicit DBNull value will be passed to all unrecognized stored procedure inputs
		private Dictionary<string, string> sqlParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

		public FileImportAdapter(IConfiguration _config,
			ILogger<FileImportAdapter> _logger,
			IMemoryCache _memorycache,
			string taskName = null)
			: base(_config, _logger, taskName)
		{
			memorycache = _memorycache;
		}
		#endregion

		#region IDisposable implementation
		protected override void Dispose(bool disposing)
		{
			if (disposed == false && disposing && md5hash != null)
			{
				md5hash.Dispose();
				md5hash = null;
			}
			disposed = true;
			base.Dispose(disposing);
		}
		#endregion

		/// <summary>
		/// TODO: DESCRIPTION
		/// </summary>
		public override async Task<TaskResult> ExecuteTask(TaskParameters parameters)
		{
			var result = new TaskResult();
			var dateTimeNow = DateTime.Now; // Use single value throughout for consistency in macro replacements
			try
			{
				#region Retrieve task parameters
				string connectionString = config.GetConnectionString(parameters.GetString("ConnectionString"));
				importProcedureName = parameters.GetString("ImportProcedureName");
				string importFolder = parameters.GetString("ImportFolder", TaskUtilities.General.REGEX_DIRPATH, dateTimeNow);
				string filenameFilter = parameters.GetString("FilenameFilter");
				if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(importProcedureName) || string.IsNullOrEmpty(importFolder) || string.IsNullOrEmpty(filenameFilter))
				{
					throw new ArgumentException("Missing or invalid: one or more of ConnectionString/ImportProcedureName/ImportFolder/FilenameFilter");
				}

				// Retrieve imported file archival settings:
				string archiveFolder = parameters.GetString("ArchiveFolder", TaskUtilities.General.REGEX_DIRPATH, dateTimeNow);
				string archiveRenameRegex = parameters.GetString("ArchiveRenameRegex");
				string archiveRenameReplacement = string.IsNullOrEmpty(archiveRenameRegex) ? null : parameters.GetString("ArchiveRenameReplacement", null, dateTimeNow);
				if (string.IsNullOrEmpty(archiveFolder))
				{
					// If archive folder not specified, regex and replacement strings are required:
					if (string.IsNullOrEmpty(archiveRenameReplacement))
					{
						throw new ArgumentException("Either ArchiveFolder or ArchiveRenameRegex/ArchiveRenameReplacement settings required");
					}
				}
				// Create archival folder, if it doesn't already exist:
				else if (!Directory.Exists(archiveFolder))
				{
					Directory.CreateDirectory(archiveFolder);
				}
				// Create regex for archival process renaming, if required:
				var rArchiveRenameRegex = string.IsNullOrEmpty(archiveRenameReplacement) ? null : new Regex(archiveRenameRegex, RegexOptions.IgnoreCase);

				// Retrieve optional parameters:
				importProcedureTimeout = parameters.Get<int>("ImportProcedureTimeout", int.TryParse);
				importPreProcessorName = parameters.GetString("ImportPreProcessorName");
				importPreProcessorTimeout = parameters.Get<int>("ImportPreProcessorTimeout", int.TryParse);
				importPostProcessorName = parameters.GetString("ImportPostProcessorName");
				importPostProcessorTimeout = parameters.Get<int>("ImportPostProcessorTimeout", int.TryParse);
				importPGPPrivateKeyRing = parameters.GetString("ImportPGPPrivateKeyRing");
				importPGPPassphrase = parameters.GetString("ImportPGPPassphrase");
				defaultNulls = parameters.GetBool("DefaultNulls");
				string filenameRegex = parameters.GetString("FilenameRegex");
				bool haltOnImportError = parameters.GetBool("HaltOnImportError");

				// Add any parameters to be applied to SQL statements to dictionary:
				var atparms = parameters.GetKeys().Where(parmname => parmname.StartsWith("@"));
				foreach (var atparm in atparms)
				{
					sqlParameters[atparm] = parameters.GetString(atparm, null, dateTimeNow);
				}

				// Create regex object from custom string, if provided (and from file filter, otherwise - this
				// check is performed to avoid false-positives on 8.3 version of filenames):
				var rFilenameRegex = string.IsNullOrEmpty(filenameRegex) ? TaskUtilities.General.RegexFromFileFilter(filenameFilter)
					: new Regex(filenameRegex, RegexOptions.IgnoreCase);
				#endregion

				// Build listing of all matching files in import folder:
				var fileInfoList = Directory.EnumerateFiles(importFolder, filenameFilter)
					.Where(fileName => rFilenameRegex.IsMatch(Path.GetFileName(fileName)))
					.Select(fileName => new FileInfo(fileName));

				if (fileInfoList.Any())
				{
					#region Connect to database and process all files in list
					using (var cnn = new SqlConnection(connectionString))
					{
						await cnn.OpenAsync();

						// Capture console messages into StringBuilder:
						var consoleOutput = new StringBuilder();
						cnn.InfoMessage += (object obj, SqlInfoMessageEventArgs e) => { consoleOutput.AppendLine(e.Message); };

						foreach (var fileInfo in fileInfoList)
						{
							using var importFileScope = logger.BeginScope(new Dictionary<string, object>() { ["FileName"] = fileInfo.FullName });

							// Determine path to archive file to after completion of import process:
							var archiveFilePath = Path.Combine(archiveFolder,
								rArchiveRenameRegex == null ? fileInfo.Name : rArchiveRenameRegex.Replace(fileInfo.Name, archiveRenameReplacement));
							if (archiveFilePath.Equals(fileInfo.FullName, StringComparison.OrdinalIgnoreCase))
							{
								logger.LogWarning($"Import and archive folders are the same, file will not be archived");
								archiveFilePath = null; // Ensure file move is skipped over below
							}

							try
							{
								// Open file in read-only mode
								using (var filestream = new FileStream(fileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.None))
								{
									// Pass processing off to utility function, receiving any output parameters from
									// import procedure and merging into overall task return value set:
									result.MergeReturnValues(await ImportFile(cnn, fileInfo, filestream));
								}
							}
							catch (Exception ex)
							{
								if (ex is AggregateException ae) // Exception caught from async task; simplify if possible
								{
									ex = TaskUtilities.General.SimplifyAggregateException(ae);
								}

								// Log error here (to take advantage of logger scope context) and add exception to result collection:
								logger.LogError(ex, "Error importing file");
								result.AddException(new Exception($"Error importing file {fileInfo.FullName}", ex));

								// If we are halting process for individual import errors, throw custom exception to indicate
								// to outer try block that it can just exit without re-logging:
								if (haltOnImportError)
								{
									archiveFilePath = null; // Prevent finally block from archiving this file
									throw new TaskExitException();
								}
							}
							finally
							{
								// Unless file archival has been explicitly cancelled, move file to archive (regardless of result):
								if (archiveFilePath != null)
								{
									try
									{
										File.Move(fileInfo.FullName, archiveFilePath);
										logger.LogDebug($"File archived to {archiveFilePath}");
									}
									catch (Exception ex)
									{
										// Add exception to response collection, do not re-throw (to avoid losing actual valuable
										// exception that may currently be throwing to outer block)
										result.AddException(new Exception($"Error archiving file {fileInfo.FullName} to {archiveFilePath}", ex));
									}
								}
								// Import procedure should have consumed any console output already; ensure it is cleared for next run:
								if (consoleOutput.Length > 0)
								{
									logger.LogDebug($"Unhandled console output follows:\n{consoleOutput}");
									consoleOutput.Clear();
								}
							}
						}
					}
					#endregion
				}
			}
			catch (TaskExitException)
			{
				// Exception was handled above - just proceed with return below
			}
			catch (Exception ex)
			{
				if (ex is AggregateException ae) // Exception caught from async task; simplify if possible
				{
					ex = TaskUtilities.General.SimplifyAggregateException(ae);
				}
				logger.LogError(ex, "Import process failed");
				result.AddException(ex);
			}
			return result;
		}

		#region Private methods
		/// <summary>
		/// Dynamically derive parameters from SQL server, and bind inputs from parameter collection
		/// </summary>
		private void DeriveAndBindParameters(SqlCommand cmd)
		{
			// Create cache key from database connection string (hashed, to avoid saving passwords/etc in collection) and stored
			// proc name, check whether parameters have already been derived for this object within current run:
			string cachekey = $"{cmd.Connection.ConnectionString.GetHashCode()}::{cmd.CommandText}";
			if (memorycache.TryGetValue<SqlParameter[]>(cachekey, out var cachedparms))
			{
				// Cached parameters available - clone cached array into destination parameter collection:
				cmd.Parameters.AddRange(cachedparms.Select(x => ((ICloneable)x).Clone()).Cast<SqlParameter>().ToArray());
			}
			else
			{
				// No cached parameters available; derive from server (note this is synchronous, no async version available):
				SqlCommandBuilder.DeriveParameters(cmd);

				// Clone parameter collection into cache (with 5 minute TTL) for subsequent calls to this same proc:
				memorycache.Set(
					cachekey,
					cmd.Parameters.Cast<SqlParameter>().Select(x => ((ICloneable)x).Clone()).Cast<SqlParameter>().ToArray(),
					new MemoryCacheEntryOptions().SetAbsoluteExpiration(TimeSpan.FromMinutes(5))
				);
			}

			#region Iterate through parameter collection, applying values from member collection
			foreach (SqlParameter sqlParameter in cmd.Parameters)
			{
				if (sqlParameter.Direction.HasFlag(ParameterDirection.Input))
				{
					// This parameter requires input value:
					bool needsDefault = false;

					// Check whether parameter is included in SQL parameter dictionary:
					if (sqlParameters.ContainsKey(sqlParameter.ParameterName))
					{
						// Special case - strings are not automatically changed to Guid values, attempt explicit conversion:
						if (sqlParameter.SqlDbType == SqlDbType.UniqueIdentifier)
						{
							if (Guid.TryParse(sqlParameters[sqlParameter.ParameterName], out var guid))
							{
								sqlParameter.Value = guid;
								needsDefault = false;
							}
						}
						else
						{
							// Apply string value to parameter (replacing null with DBNull):
							sqlParameter.Value = (object)sqlParameters[sqlParameter.ParameterName] ?? DBNull.Value;
							needsDefault = false;
						}
					}

					// If parameter value was not set above and either we are set to supply default NULL
					// values OR this is also an OUTPUT parameter, set to explicit NULL:
					if (needsDefault && (defaultNulls || sqlParameter.Direction.HasFlag(ParameterDirection.Output)))
					{
						sqlParameter.Value = DBNull.Value;
					}
					// (otherwise, value will be left unspecified; if stored procedure does not provide a default
					// value, execution will fail and missing parameter will be indicated by exception string)
				}
			}
			#endregion
		}

		private async Task<Dictionary<string, string>> ImportFile(SqlConnection cnn, FileInfo fileinfo, Stream filestream)
		{
			Console.WriteLine($"Will import {fileinfo.FullName}");
			throw new NotImplementedException();
		}
		#endregion
	}
}

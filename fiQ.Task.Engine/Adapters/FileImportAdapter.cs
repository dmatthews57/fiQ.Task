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
							var archiveFilePath = Path.Combine(string.IsNullOrEmpty(archiveFolder) ? importFolder : archiveFolder,
								rArchiveRenameRegex == null ? fileInfo.Name : rArchiveRenameRegex.Replace(fileInfo.Name, archiveRenameReplacement));
							if (archiveFilePath.Equals(fileInfo.FullName, StringComparison.OrdinalIgnoreCase))
							{
								logger.LogWarning($"Import and archive folders are the same, file will not be archived");
								archiveFilePath = null; // Ensure file move is skipped over below
							}

							try
							{
								// Create fileControlBlock to hold file information and data streams, then pass processing off to
								// utility function, receiving any output parameters from import procedure and merging into overall
								// task return value set:
								using (var fileControlBlock = new FileControlBlock(fileInfo))
								{
									result.MergeReturnValues(await ImportFile(cnn, consoleOutput, fileControlBlock));
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

				// If this point is reached, consider overall operation successful
				result.Success = true;
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
		/// Dynamically derive parameters from SQL server, and bind inputs from parameter collection, file control block
		/// and running collection of output parameters
		/// </summary>
		/// <returns>
		/// A value indicating whether structured data is present (which may require further handling)
		/// </returns>
		private async Task<bool> DeriveAndBindParameters(SqlCommand cmd, FileControlBlock fileControlBlock, Dictionary<string, object> outputParameters)
		{
			bool hasStructuredData = false;

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
				if (sqlParameter.SqlDbType == SqlDbType.Structured)
				{
					// Flag that caller will have to handle this parameter (apply valid data or remove), and set to NULL:
					hasStructuredData = true;
					sqlParameter.Value = DBNull.Value;

					// Correct for a problem with DeriveParameters, which erroneously adds database name to type:
					var typeNameParts = sqlParameter.TypeName.Split('.');
					if (typeNameParts.Length == 3) // Database name is included in three-part format, strip out
					{
						sqlParameter.TypeName = $"{typeNameParts[1]}.{typeNameParts[2]}";
					}
				}
				else if (sqlParameter.Direction.HasFlag(ParameterDirection.Input))
				{
					// Check for reserved special parameters to be supplied based on file data:
					switch (sqlParameter.ParameterName.ToUpperInvariant())
					{
						case "@FILENAME":
							sqlParameter.Value = fileControlBlock.fileInfo.Name;
							break;
						case "@FILEFOLDER":
							sqlParameter.Value = fileControlBlock.fileInfo.DirectoryName;
							break;
						case "@FILEWRITETIMEUTC":
							sqlParameter.Value = fileControlBlock.fileInfo.LastWriteTimeUtc;
							break;
						case "@FILESIZE":
							sqlParameter.Value = fileControlBlock.fileInfo.Length;
							break;
						case "@FILEHASH":
							// Create hash provider, if not already created:
							if (md5hash == null)
							{
								md5hash = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
							}
							sqlParameter.Value = await fileControlBlock.GetFileHash(md5hash) ?? DBNull.Value;
							break;
						case "@IMPORTSERVER":
							sqlParameter.Value = Environment.MachineName;
							break;
						default: // Unreserved parameter name - supply value from parameters, if possible

							// This parameter will require an input value if also an output parameter OR we are explicitly defaulting:
							bool needsDefault = (defaultNulls || sqlParameter.Direction.HasFlag(ParameterDirection.Output));

							// Check whether parameter is included in collection of output parameters from previous calls:
							if (outputParameters.ContainsKey(sqlParameter.ParameterName))
							{
								// Apply value to parameter (replacing null with DBNull):
								sqlParameter.Value = outputParameters[sqlParameter.ParameterName] ?? DBNull.Value;
								needsDefault = false;
							}
							// Check whether parameter is included in SQL parameter dictionary:
							else if (sqlParameters.ContainsKey(sqlParameter.ParameterName))
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

							// If we still need a default value, set to null (otherwise, any value not set above will be left
							// unspecified; if stored procedure does not provide a default value, execution will fail and
							// missing parameter will be indicated by exception string)
							if (needsDefault)
							{
								sqlParameter.Value = DBNull.Value;
							}
							break;
					};
				}
			}
			#endregion

			return hasStructuredData;
		}

		/// <summary>
		/// Call import procedure(s), passing source file data into database
		/// </summary>
		/// <param name="cnn">Open connection to database</param>
		/// <param name="fileControlBlock">Object containing FileInfo and an open FileStream for source file</param>
		/// <returns>Collection of output parameters from import procedure(s)</returns>
		private async Task<Dictionary<string, string>> ImportFile(SqlConnection cnn, StringBuilder consoleOutput, FileControlBlock fileControlBlock)
		{
			var outputParameters = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
			var loggerScope = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

			if (!string.IsNullOrEmpty(importPreProcessorName))
			{
				#region Execute pre-processor
				try
				{
					using (var cmd = new SqlCommand(importPreProcessorName, cnn) { CommandType = CommandType.StoredProcedure })
					{
						// Apply timeout if configured, derive and bind parameters, execute procedure:
						if (importPreProcessorTimeout > 0) cmd.CommandTimeout = (int)importPreProcessorTimeout;
						await DeriveAndBindParameters(cmd, fileControlBlock, outputParameters);
						await cmd.ExecuteNonQueryAsync();

						#region Iterate through output parameters
						int? returnValue = null;
						foreach (SqlParameter sqlParameter in cmd.Parameters)
						{
							if (sqlParameter.Direction == ParameterDirection.ReturnValue)
							{
								returnValue = sqlParameter.Value as int?;
							}
							else if (sqlParameter.Direction.HasFlag(ParameterDirection.Output))
							{
								// Check for reserved values with special behavior:
								if (sqlParameter.ParameterName.Equals("@FILEID", StringComparison.OrdinalIgnoreCase))
								{
									if (!loggerScope.ContainsKey("FileID"))
									{
										// Set logging scope so subsequent logging for this block will include this value:
										loggerScope["FileID"] = sqlParameter.Value;
										fileControlBlock.SetLoggerScope(logger, loggerScope);
									}
								}

								// Save value in output collection (as object, converting DBNull to null):
								outputParameters[sqlParameter.ParameterName] = sqlParameter.Value == DBNull.Value ? null : sqlParameter.Value;
							}
						}
						#endregion

						// Validate return code:
						if (returnValue > 0 && returnValue < 100) // Values 1-99 are reserved for warnings, will not halt import process
						{
							logger.LogWarning($"Pre-processor returned non-standard value {returnValue}, proceeding with import");
						}
						else if (returnValue != 0) // Any other non-zero value (including null) will abort process
						{
							throw new Exception($"Invalid return value ({returnValue})");
						}
						else
						{
							logger.LogDebug("Pre-processor complete");
						}
					}
				}
				catch (Exception ex)
				{
					throw new Exception("Exception executing pre-processor", ex is AggregateException ae ? TaskUtilities.General.SimplifyAggregateException(ae) : ex);
				}
				finally
				{
					if (consoleOutput.Length > 0)
					{
						logger.LogDebug($"Pre-processor console output follows:\n{consoleOutput}");
						consoleOutput.Clear();
					}
				}
				#endregion
			}

			#region Execute data import
			try
			{
				using (var cmd = new SqlCommand(importProcedureName, cnn) { CommandType = CommandType.StoredProcedure })
				{
					// Apply timeout if configured, derive and bind parameters:
					if (importProcedureTimeout > 0) cmd.CommandTimeout = (int)importProcedureTimeout;
					bool hasStructuredData = await DeriveAndBindParameters(cmd, fileControlBlock, outputParameters);

					// TODO: BIND DATA

					#region Remove any parameters with NULL unstructured data
					if (hasStructuredData)
					{
						// Any structured data parameters that are still set to DBNull must be removed (structured
						// parameters can't be null, and table can't be empty):
						var removeparms = cmd.Parameters
							.Cast<SqlParameter>()
							.Where(parm => parm.SqlDbType == SqlDbType.Structured && parm.Value == DBNull.Value);
						if (removeparms.Any())
						{
							// Create list from enumerable (since we will be removing from collection as we go, force
							// enumeration), then remove each entry by name (since ordinals will change)
							var removeparmslist = removeparms.ToList();
							foreach (var removeparm in removeparmslist)
							{
								cmd.Parameters.RemoveAt(removeparm.ParameterName);
							}
						}
					}
					#endregion

					await cmd.ExecuteNonQueryAsync();

					#region Iterate through output parameters
					int? returnValue = null;
					int? rowsImported = null;
					foreach (SqlParameter sqlParameter in cmd.Parameters)
					{
						if (sqlParameter.Direction == ParameterDirection.ReturnValue)
						{
							returnValue = sqlParameter.Value as int?;
						}
						else if (sqlParameter.Direction.HasFlag(ParameterDirection.Output))
						{
							// Check for reserved values with special behavior:
							if (sqlParameter.ParameterName.Equals("@FILEID", StringComparison.OrdinalIgnoreCase))
							{
								if (!loggerScope.ContainsKey("FileID"))
								{
									// Update logging scope so subsequent logging for this block will include this value:
									loggerScope["FileID"] = sqlParameter.Value;
									fileControlBlock.SetLoggerScope(logger, loggerScope);
								}
							}
							else if (sqlParameter.ParameterName.Equals("@ROWSIMPORTED", StringComparison.OrdinalIgnoreCase))
							{
								rowsImported = sqlParameter.Value as int?;
							}

							// Save value in output collection (as object, converting DBNull to null):
							outputParameters[sqlParameter.ParameterName] = sqlParameter.Value == DBNull.Value ? null : sqlParameter.Value;
						}
					}
					#endregion

					// Validate return code:
					if (returnValue > 0 && returnValue < 100) // Values 1-99 are reserved for warnings, will not halt import process
					{
						logger.LogWarning($"Import returned non-standard value {returnValue}, proceeding");
					}
					else if (returnValue != 0) // Any other non-zero value (including null) will abort process
					{
						throw new Exception($"Invalid return value ({returnValue})");
					}
					else
					{
						logger.LogInformation((rowsImported == null) ? "Import complete" : $"Import complete, {rowsImported} rows imported");
					}
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Exception executing import", ex is AggregateException ae ? TaskUtilities.General.SimplifyAggregateException(ae) : ex);
			}
			finally
			{
				if (consoleOutput.Length > 0)
				{
					logger.LogDebug($"Import console output follows:\n{consoleOutput}");
					consoleOutput.Clear();
				}
			}
			#endregion

			if (!string.IsNullOrEmpty(importPostProcessorName))
			{
				#region Execute post-processor
				try
				{
					using (var cmd = new SqlCommand(importPostProcessorName, cnn) { CommandType = CommandType.StoredProcedure })
					{
						// Apply timeout if configured, derive and bind parameters, execute procedure:
						if (importPostProcessorTimeout > 0) cmd.CommandTimeout = (int)importPostProcessorTimeout;
						await DeriveAndBindParameters(cmd, fileControlBlock, outputParameters);
						await cmd.ExecuteNonQueryAsync();

						#region Iterate through output parameters
						int? returnValue = null;
						foreach (SqlParameter sqlParameter in cmd.Parameters)
						{
							if (sqlParameter.Direction == ParameterDirection.ReturnValue)
							{
								returnValue = sqlParameter.Value as int?;
							}
							else if (sqlParameter.Direction.HasFlag(ParameterDirection.Output))
							{
								// Check for reserved values with special behavior:
								if (sqlParameter.ParameterName.Equals("@FILEID", StringComparison.OrdinalIgnoreCase))
								{
									if (!loggerScope.ContainsKey("FileID"))
									{
										// Update logging scope so subsequent logging for this block will include this value:
										loggerScope["FileID"] = sqlParameter.Value;
										fileControlBlock.SetLoggerScope(logger, loggerScope);
									}

									// Save value in output collection (as object, converting DBNull to null):
									outputParameters[sqlParameter.ParameterName] = sqlParameter.Value == DBNull.Value ? null : sqlParameter.Value;
								}
							}
						}
						#endregion

						// Validate return code:
						if (returnValue > 0 && returnValue < 100) // Values 1-99 are reserved for warnings, will not halt import process
						{
							logger.LogWarning($"Post-processor returned non-standard value {returnValue}");
						}
						else if (returnValue != 0) // Any other non-zero value (including null) will abort process
						{
							throw new Exception($"Invalid return value ({returnValue})");
						}
						else
						{
							logger.LogDebug("Post-processor complete");
						}
					}
				}
				catch (Exception ex)
				{
					throw new Exception("Exception executing post-processor", ex is AggregateException ae ? TaskUtilities.General.SimplifyAggregateException(ae) : ex);
				}
				finally
				{
					if (consoleOutput.Length > 0)
					{
						logger.LogDebug($"Post-processor console output follows:\n{consoleOutput}");
						consoleOutput.Clear();
					}
				}
				#endregion
			}

			// Return output collection (converting objects to strings), to be added to task return values:
			return outputParameters.ToDictionary(entry => entry.Key, entry => entry.Value?.ToString());
		}
		#endregion

		#region Private classes
		/// <summary>
		/// Container class to hold (and dispose of) source file information and handles to data
		/// </summary>
		private class FileControlBlock : IDisposable
		{
			#region Fields and constructors
			private bool disposed = false;
			private Stream filestream = null;
			private TaskUtilities.StreamStack decryptionstream = null;
			private byte[] fileHash = null;
			private IDisposable loggerScope = null;
			public FileControlBlock(FileInfo _fileInfo)
			{
				fileInfo = _fileInfo;
				filestream = new FileStream(fileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.None);
			}
			#endregion

			#region IDisposable implementation
			public void Dispose()
			{
				Dispose(true);
				GC.SuppressFinalize(this);
			}
			protected virtual void Dispose(bool disposing)
			{
				if (disposed == false && disposing)
				{
					if (decryptionstream != null)
					{
						decryptionstream.Dispose();
						decryptionstream = null;
					}
					if (filestream != null)
					{
						filestream.Dispose();
						filestream = null;
					}
					if (loggerScope != null)
					{
						loggerScope.Dispose();
						loggerScope = null;
					}
				}
				disposed = true;
			}
			#endregion

			#region Properties
			public FileInfo fileInfo { get; }
			#endregion

			#region Methods
			/// <summary>
			/// Add logger scope specific to processing of this file
			/// </summary>
			public void SetLoggerScope(ILogger logger, Dictionary<string, object> state)
			{
				// Dispose of existing scope, if any:
				if (loggerScope != null)
				{
					loggerScope.Dispose();
					loggerScope = null;
				}

				// Begin new logging scope with updated state:
				loggerScope = logger.BeginScope(state);
			}

			/// <summary>
			/// Get readable input stream for file data
			/// </summary>
			public Stream GetStream()
			{
				// If decryptionstream is empty (or does not exist), return raw stream:
				return (decryptionstream?.Empty ?? true) ? filestream : decryptionstream.GetStream();
			}

			/// <summary>
			/// Retrieve hash of file contents, using the specified provider
			/// </summary>
			public async Task<object> GetFileHash(IncrementalHash hashprovider)
			{
				// If hash has not already been calculated and no decryption stream is in use, calculate
				// hash now and reset stream position to zero (if decryption stream has been created,
				// we don't want to mess with the underlying filestream):
				if (fileHash == null && decryptionstream == null)
				{
					fileHash = await TaskUtilities.Streams.GetStreamHash(filestream, hashprovider);
					filestream.Position = 0;
				}
				return fileHash;
			}
			#endregion
		}
		#endregion
	}
}

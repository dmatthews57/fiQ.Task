using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using fiQ.TaskModels;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic.FileIO;

namespace fiQ.TaskAdapters
{
	public class FileImportAdapter : TaskAdapter
	{
		#region Fields and constructors
		// Private static fields
		private static readonly Regex ImportModeValidationRegex = new Regex(@"^(RAW|XML|CSV)$");

		// Private fields
		private IMemoryCache memorycache;   // Injected by constructor, used to cache SQL stored procedure parameters
		private IncrementalHash md5hash = null;
		private bool disposed = false;

		// Private configuration parameter fields
		private string importProcedureName = null;		// Stored procedure for actual data import
		private int? importProcedureTimeout = null;     // Optional timeout (in seconds) for main import procedure call
		private string importMode = null;               // Optional, will override default behavior of structured data table
		private string importDataParameterName = null;	// Optional, specifies stored procedure parameter to receive file contents
		private string importPreProcessorName = null;	// Optional stored procedure for pre-processing file
		private int? importPreProcessorTimeout = null;	// Optional timeout (in seconds) for pre-processor call
		private string importPostProcessorName = null;	// Optional stored procedure for post-processing file
		private int? importPostProcessorTimeout = null;	// Optional timeout (in seconds) for post-processor call
		private string importPGPPrivateKeyRing = null;	// If present, data input will be PGP-decrypted with a key from the specified ring
		private string importPGPPassphrase = null;      // If PGP private keyring specified, passphrase to access private key
		private bool defaultNulls = false;              // If true, explicit DBNull value will be passed to all unrecognized stored procedure inputs
		private string delimiter = null;                // Used for ImportMode "CSV", when parsing source data (default comma)
		private Regex importLineFilterRegex = null;		// If specified, only lines matching this regex will be imported ("RAW"/default mode only)
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
		/// Retrieve all files from the specified location (matching specified pattern), and import their contents
		/// into the provided database using the stored procedure(s) specified
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
				string filenameFilter = parameters.GetString("FilenameFilter", null, dateTimeNow);
				if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(importProcedureName) || string.IsNullOrEmpty(importFolder) || string.IsNullOrEmpty(filenameFilter))
				{
					throw new ArgumentException("Missing or invalid: one or more of ConnectionString/ImportProcedureName/ImportFolder/FilenameFilter");
				}

				// Retrieve import mode (default is RAW), validate if present:
				importMode = parameters.GetString("ImportMode")?.ToUpperInvariant();
				if (!string.IsNullOrEmpty(importMode))
				{
					if (!ImportModeValidationRegex.IsMatch(importMode))
					{
						throw new ArgumentException("Invalid ImportMode");
					}
				}
				// Retrieve data parameter name, validate if present:
				importDataParameterName = parameters.GetString("ImportDataParameterName");
				if (!string.IsNullOrEmpty(importDataParameterName))
				{
					if (!importDataParameterName.StartsWith("@"))
					{
						throw new ArgumentException("Invalid ImportDataParameterName");
					}
				}

				// Retrieve imported file archival settings (either folder or rename regex required):
				string archiveFolder = parameters.GetString("ArchiveFolder", TaskUtilities.General.REGEX_DIRPATH, dateTimeNow);
				var archiveRenameRegex = TaskUtilities.General.RegexIfPresent(parameters.GetString("ArchiveRenameRegex"), RegexOptions.IgnoreCase);
				string archiveRenameReplacement = archiveRenameRegex == null ? null : (parameters.GetString("ArchiveRenameReplacement", null, dateTimeNow) ?? string.Empty);
				if (string.IsNullOrEmpty(archiveFolder) && archiveRenameRegex == null)
				{
					throw new ArgumentException("Either ArchiveFolder or ArchiveRenameRegex settings required");
				}
				// Create archival folder, if it doesn't already exist:
				else if (!Directory.Exists(archiveFolder))
				{
					Directory.CreateDirectory(archiveFolder);
				}

				// Retrieve optional parameters:
				importProcedureTimeout = parameters.Get<int>("ImportProcedureTimeout", int.TryParse);
				importPreProcessorName = parameters.GetString("ImportPreProcessorName");
				importPreProcessorTimeout = parameters.Get<int>("ImportPreProcessorTimeout", int.TryParse);
				importPostProcessorName = parameters.GetString("ImportPostProcessorName");
				importPostProcessorTimeout = parameters.Get<int>("ImportPostProcessorTimeout", int.TryParse);
				importPGPPrivateKeyRing = parameters.GetString("ImportPGPPrivateKeyRing");
				importPGPPassphrase = parameters.GetString("ImportPGPPassphrase");
				defaultNulls = parameters.GetBool("DefaultNulls");
				var filenameRegex = TaskUtilities.General.RegexIfPresent(parameters.GetString("FilenameRegex"), RegexOptions.IgnoreCase);
				delimiter = parameters.GetString("Delimiter");
				if (string.IsNullOrEmpty(delimiter))
				{
					delimiter = ",";
				}
				importLineFilterRegex = TaskUtilities.General.RegexIfPresent(parameters.GetString("ImportLineFilterRegex"));
				bool haltOnImportError = parameters.GetBool("HaltOnImportError");

				// Add any parameters to be applied to SQL statements to dictionary:
				var atparms = parameters.GetKeys().Where(parmname => parmname.StartsWith("@"));
				foreach (var atparm in atparms)
				{
					sqlParameters[atparm] = parameters.GetString(atparm, null, dateTimeNow);
				}

				// If custom regex not specified, create one from file filter (this check is performed to avoid false-positives on 8.3 version of filenames):
				if (filenameRegex == null)
				{
					filenameRegex = TaskUtilities.General.RegexFromFileFilter(filenameFilter);
				}
				#endregion

				// Build listing of all matching files in import folder:
				var fileInfoList = Directory.EnumerateFiles(importFolder, filenameFilter)
					.Where(fileName => filenameRegex.IsMatch(Path.GetFileName(fileName)))
					.Select(fileName => new FileInfo(fileName));

				if (fileInfoList.Any())
				{
					#region Connect to database and process all files in list
					await using (var cnn = new SqlConnection(connectionString))
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
								archiveRenameRegex == null ? fileInfo.Name : archiveRenameRegex.Replace(fileInfo.Name, archiveRenameReplacement));
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
		/// A dictionary of potential data parameters (all structured data parameters, add XML inputs if mode is XML)
		/// </returns>
		private async Task<Dictionary<string,SqlDbType>> DeriveAndBindParameters(SqlCommand cmd, FileControlBlock fileControlBlock, Dictionary<string, object> outputParameters)
		{
			var dataParameters = new Dictionary<string, SqlDbType>(StringComparer.OrdinalIgnoreCase);

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
					// Add this parameter to return collection and set input to null (caller must apply valid data or remove):
					dataParameters[sqlParameter.ParameterName] = SqlDbType.Structured;
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
					// If this is an XML field and we are processing in XML input mode, add to return collection:
					if (sqlParameter.SqlDbType == SqlDbType.Xml && importMode == "XML")
					{
						dataParameters[sqlParameter.ParameterName] = SqlDbType.Xml;
					}

					// Check for reserved special parameters to be supplied based on file data:
					switch (sqlParameter.ParameterName.ToUpperInvariant())
					{
						case "@FILENAME":
							sqlParameter.Value = fileControlBlock.fileInfo.Name;
							break;
						case "@FILEFOLDER":
							sqlParameter.Value = fileControlBlock.fileInfo.DirectoryName;
							break;
						case "@FILEWRITETIME":
							sqlParameter.Value = fileControlBlock.fileInfo.LastWriteTime;
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

			return dataParameters;
		}

		/// <summary>
		/// Stream data from source into IEnumerable to be applied to SQL datatable
		/// </summary>
		/// <remarks>
		/// Note that this function will execute source reads synchronously (during actual stored procedure execution)
		/// </remarks>
		private IEnumerable<SqlDataRecord> StreamDataTable(StreamReader rawReader = null, TextFieldParser csvReader = null)
		{
			// Set up metadata object matching user-defined type in SQL (contains row identifier column and RowData varchar(max) column);
			// this could be made configurable, but would require some complex logic below to properly map columns:
			var udt_ImportTable_Schema = new SqlMetaData[] {
				new SqlMetaData("RowID", SqlDbType.Int),
				new SqlMetaData("RowData", SqlDbType.VarChar, SqlMetaData.Max)
			};

			// Create record to populate (SQL server will receive each row when "yield return" is called, meaning
			// we can just update this inputRecord with new data for each input row):
			var inputRecord = new SqlDataRecord(udt_ImportTable_Schema);
			int rowCount = 1;

			// If specialized CSV reader provided, use TextFieldParser to read all available data and pass to SQL in custom-delimited
			// string (note this could be made configurable, using a custom input table type with discrete columns rather than a single
			// delimited string data column):
			if (csvReader != null)
			{
				var csvFieldString = new StringBuilder();
				while (!csvReader.EndOfData)
				{
					inputRecord.SetInt32(0, rowCount++);
					// Read separated columns from input source, and construct string using custom delimiter "|::|". Note that
					// SQL will need to be prepared to re-tokenize string using this delimiter, and de-escape "|::" with "|:"
					// only AFTER doing so (also note that any fields absent in source data will be absent in input row - i.e.
					// number of column/fields could be inconsistent between rows, meaning that SQL must be prepared for
					// "ragged right" type data and treat missing columns accordingly):
					var csvFields = csvReader.ReadFields();
					for (int i = 0; i < csvFields.Length; ++i)
					{
						csvFieldString.Append(csvFields[i].Replace("|:", "|::"));
						csvFieldString.Append("|::|");
					}
					inputRecord.SetString(1, csvFieldString.ToString());
					yield return inputRecord;
					csvFieldString.Clear();
				}
			}
			// Otherwise if raw StreamReader provided, just read all lines as-is:
			else if (rawReader != null)
			{
				while (!rawReader.EndOfStream)
				{
					inputRecord.SetInt32(0, rowCount++);
					inputRecord.SetString(1, rawReader.ReadLine());

					// If line filter regex configured, only actually return this line if it matches (note: with regex specified,
					// it is possible that no lines will match and IEnumerable will be empty - this will cause an exception to be
					// thrown at execution time, requiring a null collection rather than an empty one):
					if (importLineFilterRegex == null ? true : importLineFilterRegex.IsMatch(inputRecord.GetString(1)))
					{
						yield return inputRecord;
					}
				}
			}
			else
			{
				throw new ArgumentException("No reader object supplied to stream to data table");
			}
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
					await using var cmd = new SqlCommand(importPreProcessorName, cnn) { CommandType = CommandType.StoredProcedure };
					using var preproclogscope = logger.BeginScope(new Dictionary<string, object>() { ["PreProcessorProcedure"] = importPreProcessorName });
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
			TextFieldParser filecsvreader = null;   // Used for CSV import method
			StreamReader filestreamreader = null;   // Used for default import method
			try
			{
				// If PGP decryption of source file is required, update read stream in FileControlBlock now:
				if (!string.IsNullOrEmpty(importPGPPrivateKeyRing))
				{
					using var privatekeystream = new FileStream(importPGPPrivateKeyRing, FileMode.Open, FileAccess.Read, FileShare.Read);
					fileControlBlock.OpenDecryptionStream(privatekeystream, importPGPPassphrase);
				}

				// Create and execute import command object
				await using var cmd = new SqlCommand(importProcedureName, cnn) { CommandType = CommandType.StoredProcedure };
				using var proclogscope = logger.BeginScope(new Dictionary<string, object>() { ["ImportProcedure"] = importProcedureName });
				// Apply timeout if configured, derive and bind parameters:
				if (importProcedureTimeout > 0) cmd.CommandTimeout = (int)importProcedureTimeout;
				var dataParameters = await DeriveAndBindParameters(cmd, fileControlBlock, outputParameters);

				#region Bind data parameter
				// Choose data parameter to receive input; start by retrieving all parameter names for the appropriate type:
				var eligibleparms = dataParameters
					.Where(entry => entry.Value == (importMode == "XML" ? SqlDbType.Xml : SqlDbType.Structured))
					.Select(entry => entry.Key)
					// If importDataParameterName is specified, select matching entry only:
					.Where(parmname => string.IsNullOrEmpty(importDataParameterName) || parmname.Equals(importDataParameterName, StringComparison.OrdinalIgnoreCase))
					// We have to access Count argument more than once, so harden to list to prevent multiple iterations:
					.ToList();

				// If no parameters found (either no inputs of correct type, or specific importDataParameterName not found), throw error:
				if (eligibleparms.Count == 0)
				{
					throw new ArgumentException(string.IsNullOrEmpty(importDataParameterName) ?
						"No eligible input parameter found" : $"Input parameter name {importDataParameterName} not found");
				}
				else if (eligibleparms.Count > 1)
				{
					// If more than one eligible parameter was found, config did not specify; unless we have the specific case where exactly
					// one parameter has not had a value supplied already, we have no way of deciding which parameter to use:
					eligibleparms = eligibleparms.Where(parmname => cmd.Parameters[parmname].Value == null).ToList();
					if (eligibleparms.Count != 1)
					{
						throw new ArgumentException("More than one eligible data parameter found, ImportDataParameterName must be specified");
					}
				}

				if (importMode == "XML")
				{
					cmd.Parameters[eligibleparms[0]].Value = new SqlXml(fileControlBlock.GetStream());
				}
				else if (fileControlBlock.fileInfo.Length > 0) // Ignore empty files
				{
					if (importMode == "CSV") // Stream in CSV data
					{
						filecsvreader = new TextFieldParser(fileControlBlock.GetStream())
						{
							Delimiters = new string[] { delimiter },
							HasFieldsEnclosedInQuotes = true
						};
						cmd.Parameters[eligibleparms[0]].Value = StreamDataTable(csvReader: filecsvreader);
					}
					else // Stream in raw format data
					{
						filestreamreader = new StreamReader(fileControlBlock.GetStream());
						cmd.Parameters[eligibleparms[0]].Value = StreamDataTable(rawReader: filestreamreader);
					}
				}
				#endregion

				#region Remove any remaining parameters with null unstructured data
				// Structured parameters cannot be null, and datasets cannot be empty; retrieve all structured data fields
				// whose parameter value is still null, and remove from parameter collection entirely:
				var removeparms = dataParameters
					.Where(entry => entry.Value == SqlDbType.Structured)
					.Select(entry => entry.Key)
					.Where(fieldname => cmd.Parameters[fieldname].Value == DBNull.Value);
				foreach (var removeparm in removeparms)
				{
					cmd.Parameters.RemoveAt(removeparm);
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
					logger.LogWarning(rowsImported == null ? $"Import returned non-standard value {returnValue}, proceeding"
						: $"Import returned non-standard value {returnValue} with {rowsImported} rows imported, proceeding");
				}
				else if (returnValue != 0) // Any other non-zero value (including null) will abort process
				{
					throw new Exception($"Invalid return value ({returnValue})");
				}
				else
				{
					logger.LogInformation(rowsImported == null ? "Import complete" : $"Import complete, {rowsImported} rows imported");
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

				// Clean up raw and CSV mode reader objects (can't use using statements since these are conditionally initialized
				// in a different scope from their use, and need to survive until after ExecuteNonQueryAsync call completes)
				if (filecsvreader != null)
				{
					filecsvreader.Close();
					filecsvreader = null;
				}
				if (filestreamreader != null)
				{
					filestreamreader.Dispose();
					filestreamreader = null;
				}
			}
			#endregion

			if (!string.IsNullOrEmpty(importPostProcessorName))
			{
				#region Execute post-processor
				try
				{
					await using var cmd = new SqlCommand(importPostProcessorName, cnn) { CommandType = CommandType.StoredProcedure };
					using var postproclogscope = logger.BeginScope(new Dictionary<string, object>() { ["PostProcessorProcedure"] = importPostProcessorName });
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

			public void OpenDecryptionStream(Stream privatekeysource, string privatekeypassphrase)
			{
				// In the event decryption stream is already open (should never happen), dispose first:
				if (decryptionstream != null)
				{
					decryptionstream.Dispose();
					decryptionstream = null;
				}

				// Open decryption stream around filestream, using provided private key data:
				decryptionstream = TaskUtilities.Pgp.GetDecryptionStream(privatekeysource, privatekeypassphrase, filestream);
			}
			#endregion
		}
		#endregion
	}
}

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using fiQ.TaskModels;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.TaskAdapters
{
	public class FileExportAdapter : TaskAdapter
	{
		#region Fields and constructors
		private IMemoryCache memorycache;	// Injected by constructor, used to cache SQL stored procedure parameters

		private string exportProcedureName = null;		// Stored procedure for actual data export
		private int? exportProcedureTimeout = null;		// Optional timeout (in seconds) for each export procedure call
		private string exportPGPPublicKeyRing = null;	// If present, data output will be PGP-encrypted with a key from the specified ring
		private string exportPGPUserID = null;          // If using PGP public keyring, specifies UserID for key to be used from ring (if not specified, first available key will be used)
		private bool exportPGPRawFormat = false;		// If using PGP encryption, specifies raw format (default is ASCII-armored)
		private bool defaultNulls = false;				// If true, explicit DBNull value will be passed to all unrecognized stored procedure inputs
		private bool suppressIfEmpty = false;			// If true, files with no content will not be produced (and empty datasets will not generate errors)
		private bool ignoreUnexpectedTables = false;	// If true, unexpected result sets (i.e. additional result sets beyond list of export filenames) will be ignored
		private bool suppressHeaders = false;			// If true, output files will not include CSV headers
		private bool qualifyStrings = false;			// If true, all string outputs will be qualified (wrapped in quotes)
		private string delimiter = null;                // Used when outputting multiple columns (default comma)
		private Dictionary<string, string> sqlParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		private OutputColumnTemplateListSet outputColumnListSet = null;

		private static readonly Regex stringFormatRegex = new Regex(@"^[^{]*{0(,-?\d+)?(:.+)?}[^{]*$"); // string.Format style with single argument and optional formatting
		private static readonly Regex qualifiedStringRegex = new Regex("^\".*\"$"); // String is already wrapped in double-quotes
		private static readonly Regex quoteEscapeRegex = new Regex("\"?\""); // Detect double-quote, optionally preceded by double-quote (i.e. already escaped)

		public FileExportAdapter(IConfiguration _config,
			ILogger<FileExportAdapter> _logger,
			IMemoryCache _memorycache,
			string taskName = null)
			: base(_config, _logger, taskName)
		{
			memorycache = _memorycache;
		}
		#endregion

		/// <summary>
		/// Use the specified database connection string and stored procedure to export data to files (optionally
		/// retrieving a queue of files to be exported, performing multiple exports in order)
		/// </summary>
		public override async Task<TaskResult> ExecuteTask(TaskParameters parameters)
		{
			var result = new TaskResult();
			var dateTimeNow = DateTime.Now; // Use single value throughout for consistency in macro replacements
			try
			{
				#region Retrieve task parameters
				string connectionString = config.GetConnectionString(parameters.GetString("ConnectionString"));
				exportProcedureName = parameters.GetString("ExportProcedureName");
				string exportFolder = parameters.GetString("ExportFolder", TaskUtilities.General.REGEX_DIRPATH, dateTimeNow);
				if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(exportProcedureName) || string.IsNullOrEmpty(exportFolder))
				{
					throw new ArgumentException("Missing or invalid ConnectionString, ExportProcedureName and/or ExportFolder");
				}
				// Create destination folder, if it doesn't already exist:
				else if (!Directory.Exists(exportFolder))
				{
					Directory.CreateDirectory(exportFolder);
				}

				// Check for a queue monitoring procedure - if found, adapter will call specified procedure to retrieve
				// a listing of files to be exported as part of this operation:
				string queueProcedureName = parameters.GetString("QueueProcedureName");
				var queueProcedureTimeout = parameters.Get<int>("QueueProcedureTimeout", int.TryParse);

				// Retrieve other optional parameters:
				exportProcedureTimeout = parameters.Get<int>("ExportProcedureTimeout", int.TryParse);
				exportPGPPublicKeyRing = parameters.GetString("ExportPGPPublicKeyRing");
				exportPGPUserID = parameters.GetString("ExportPGPUserID");
				exportPGPRawFormat = parameters.GetBool("ExportPGPRawFormat");
				defaultNulls = parameters.GetBool("DefaultNulls");
				suppressIfEmpty = parameters.GetBool("SuppressIfEmpty");
				ignoreUnexpectedTables = parameters.GetBool("IgnoreUnexpectedTables");
				suppressHeaders = parameters.GetBool("SuppressHeaders");
				qualifyStrings = parameters.GetBool("QualifyStrings");
				delimiter = parameters.GetString("Delimiter");
				if (string.IsNullOrEmpty(delimiter))
				{
					delimiter = ",";
				}
				bool haltOnExportError = parameters.GetBool("HaltOnExportError");

				// Retrieve default destination filename (if present, will override database-provided value):
				string exportFilename = parameters.GetString("ExportFilename", null, dateTimeNow);

				// Add any parameters to be applied to SQL statements to dictionary:
				var atparms = parameters.GetKeys().Where(parmname => parmname.StartsWith("@"));
				foreach (var atparm in atparms)
				{
					sqlParameters[atparm] = parameters.GetString(atparm, null, dateTimeNow);
				}

				// Finally, read explicit column data from configuration root, if available (if section exists
				// and has content, failure to build column configuration will halt processing; if section does
				// not exist, we will proceed with default/dynamic data output):
				var outputColumnConfig = parameters.Configuration.GetSection("OutputColumns");
				if (outputColumnConfig.Exists())
				{
					outputColumnListSet = outputColumnConfig.Get<OutputColumnTemplateListSet>().EnsureValid();
				}
				#endregion

				// Open database connection
				using (var cnn = new SqlConnection(connectionString))
				{
					await cnn.OpenAsync();

					// Capture console messages into StringBuilder:
					var consoleOutput = new StringBuilder();
					cnn.InfoMessage += (object obj, SqlInfoMessageEventArgs e) => { consoleOutput.AppendLine(e.Message); };

					// Build queue of files to be exported; if no queue check procedure configured, export single default only:
					var QueuedFiles = new Queue<QueuedFile>();
					if (string.IsNullOrEmpty(queueProcedureName))
					{
						QueuedFiles.Enqueue(new QueuedFile());
						haltOnExportError = true; // Since we are only exporting single file, ensure exceptions are passed out
					}
					else
					{
						#region Execute specified procedure to retrieve pending files for export
						using var queueScope = logger.BeginScope(new Dictionary<string, object>() { ["QueueCheckProcedure"] = queueProcedureName });
						try
						{
							using var cmd = new SqlCommand(queueProcedureName, cnn) { CommandType = CommandType.StoredProcedure };
							if (queueProcedureTimeout > 0) cmd.CommandTimeout = (int)queueProcedureTimeout;

							DeriveAndBindParameters(cmd);

							#region Execute procedure and read results into queue
							using (var dr = await cmd.ExecuteReaderAsync())
							{
								while (await dr.ReadAsync())
								{
									if (!await dr.IsDBNullAsync(0)) // First column (FileID) must be non-NULL
									{
										QueuedFiles.Enqueue(new QueuedFile
										{
											FileID = await dr.GetFieldValueAsync<int>(0),
											// Only read second column (optional subfolder) if present in dataset
											Subfolder = dr.VisibleFieldCount > 1 ?
												(await dr.IsDBNullAsync(1) ? null : await dr.GetFieldValueAsync<string>(1)) : null
										});
									}
									else
									{
										logger.LogWarning("Discarded row from queue dataset (null FileID)");
									}
								}
							}
							#endregion

							var returnValue = cmd.Parameters["@RETURN_VALUE"].Value as int?;
							if (returnValue != 0)
							{
								throw new Exception($"Invalid return value ({returnValue})");
							}
							else if (QueuedFiles.Count > 0)
							{
								logger.LogDebug($"{QueuedFiles.Count} files ready to export");
							}
						}
						catch (Exception ex)
						{
							// Log error here (to take advantage of logger scope context) and add exception to result collection,
							// then throw custom exception to indicate to outer try block that it can just exit without re-logging
							logger.LogError(ex, "Queue check failed");
							result.AddException(new Exception($"Queue check procedure {queueProcedureName} failed", ex));
							throw new TaskExitException();
						}
						finally
						{
							// If queue check procedure produced console output, log now:
							if (consoleOutput.Length > 0)
							{
								logger.LogDebug($"Console output follows:\n{consoleOutput}");
								consoleOutput.Clear();
							}
						}
						#endregion
					}

					#region Process all files in queue
					while (QueuedFiles.TryDequeue(out var queuedFile))
					{
						using var exportFileScope = logger.BeginScope(new Dictionary<string, object>()
						{
							["FileID"] = queuedFile.FileID,
							["ExportProcedure"] = exportProcedureName
						});

						// If queue entry's FileID is not null, override value in parameter collection:
						if (queuedFile.FileID != null)
						{
							sqlParameters["@FileID"] = queuedFile.FileID.ToString();
						}

						try
						{
							// Pass processing off to utility function, receiving any output parameters from
							// export procedure and merging into overall task return value set:
							result.MergeReturnValues(await ExportFile(cnn, string.IsNullOrEmpty(queuedFile.Subfolder) ? exportFolder : Path.Combine(exportFolder, queuedFile.Subfolder), exportFilename));
						}
						catch (Exception ex)
						{
							if (ex is AggregateException ae) // Exception caught from async task; simplify if possible
							{
								ex = TaskUtilities.General.SimplifyAggregateException(ae);
							}

							// Log error here (to take advantage of logger scope context) and add exception to result collection:
							logger.LogError(ex, "Error exporting file");
							result.AddException(new Exception($"Error exporting file{(queuedFile.FileID == null ? string.Empty : $" (FileID {queuedFile.FileID}")}", ex));

							// If we are halting process for individual export errors, throw custom exception to indicate
							// to outer try block that it can just exit without re-logging:
							if (haltOnExportError)
							{
								throw new TaskExitException();
							}
						}
						finally
						{
							// If export procedure produced console output, log now:
							if (consoleOutput.Length > 0)
							{
								logger.LogDebug($"Console output follows:\n{consoleOutput}");
								consoleOutput.Clear();
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
				logger.LogError(ex, "Export process failed");
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
					// This parameter will require an input value if also an output parameter OR we are explicitly defaulting:
					bool needsDefault = (defaultNulls || sqlParameter.Direction.HasFlag(ParameterDirection.Output));

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

					// If we still need a default value, set to null (otherwise, any value not set above will be left
					// unspecified; if stored procedure does not provide a default value, execution will fail and
					// missing parameter will be indicated by exception string)
					if (needsDefault)
					{
						sqlParameter.Value = DBNull.Value;
					}
				}
			}
			#endregion
		}

		/// <summary>
		/// Call export procedure and write resulting content to target file(s)
		/// </summary>
		/// <param name="cnn">Open connection to database</param>
		/// <param name="exportFolder">Target folder for output files</param>
		/// <param name="exportFilename">Optional filename (or string of filenames), if present will override DB-provided value</param>
		/// <returns>Collection of output parameters from export procedure</returns>
		private async Task<Dictionary<string, string>> ExportFile(SqlConnection cnn, string exportFolder, string exportFilename)
		{
			// Create destination folder, if it does not already exist:
			if (!Directory.Exists(exportFolder))
			{
				Directory.CreateDirectory(exportFolder);
			}

			var dataSet = new DataSet();
			var outputParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

			#region Execute export procedure to populate dataset and output collection
			using (var cmd = new SqlCommand(exportProcedureName, cnn) { CommandType = CommandType.StoredProcedure })
			{
				if (exportProcedureTimeout > 0) cmd.CommandTimeout = (int)exportProcedureTimeout;

				DeriveAndBindParameters(cmd);

				using (var dr = await cmd.ExecuteReaderAsync())
				{
					// If stored procedure did not output any result sets, there will be no fields in reader;
					// otherwise iterate through all results in set and populate DataSet (note: all of this logic
					// is currently required because SqlDataAdapter does not provide an async "Fill" method; it
					// may be possible to replace this block entirely if this changes in the future)
					if (dr.VisibleFieldCount > 0)
					{
						int i = 0;
						do
						{
							var dataTable = new DataTable($"DataTable{i++}");

							#region Construct DataTable column collection from reader schema
							var schemaTable = await dr.GetSchemaTableAsync();
							foreach (DataRow schemarow in schemaTable.Rows)
							{
								var column = new DataColumn((string)schemarow["ColumnName"], (Type)schemarow["DataType"]);

								// If provider-specific data typing is available, save as extended property:
								if (schemaTable.Columns.Contains("ProviderSpecificDataType"))
								{
									if (schemarow["ProviderSpecificDataType"] is Type type)
									{
										column.ExtendedProperties.Add("ProviderType", type);
									}
								}
								dataTable.Columns.Add(column);
							}
							#endregion

							#region Read all rows from this result set into DataTable and add to DataSet
							var values = new object[dataTable.Columns.Count];
							while (await dr.ReadAsync())
							{
								dr.GetValues(values);
								dataTable.Rows.Add(values);
							}
							#endregion

							dataSet.Tables.Add(dataTable);
						}
						while (await dr.NextResultAsync());
					}
				}

				#region Retrieve output parameters into local collection
				int? returnValue = null;
				foreach (SqlParameter sqlParameter in cmd.Parameters)
				{
					if (sqlParameter.Direction == ParameterDirection.ReturnValue)
					{
						returnValue = sqlParameter.Value as int?;
					}
					else if (sqlParameter.Direction.HasFlag(ParameterDirection.Output))
					{
						outputParameters[sqlParameter.ParameterName] = sqlParameter.Value == DBNull.Value ? null : sqlParameter.Value?.ToString();
					}
				}
				#endregion

				// Validate return code:
				if (returnValue > 0 && returnValue < 100)
				{
					// Values 1-99 are reserved for warnings, will not halt export
					logger.LogWarning($"Export procedure returned non-standard value {returnValue}, proceeding with export");
				}
				else if (returnValue != 0)
				{
					throw new Exception($"Invalid return value ({returnValue})");
				}
				// If no tables were returned (not even empty ones), throw error unless we are suppressing empty datasets anyway:
				else if (dataSet.Tables.Count == 0)
				{
					return suppressIfEmpty ? outputParameters : throw new Exception("No results read");
				}
			}
			#endregion

			// Determine output filename(s) to use - if no specific value was provided by configuration,
			// export procedure is responsible for providing via output parameter:
			if (string.IsNullOrEmpty(exportFilename))
			{
				exportFilename = outputParameters.ContainsKey("@ExportFilename") ? outputParameters["@ExportFilename"] : null;
			}
			// Otherwise, configuration value will override output parameter (if any):
			else
			{
				outputParameters["@ExportFilename"] = exportFilename;
			}

			// Build collection of export filenames from filename string selected above:
			var exportFilenames = string.IsNullOrEmpty(exportFilename) ? new Queue<string>() : new Queue<string>(exportFilename.Split(';'));

			#region Iterate through all datatables in dataset, writing contents to files
			for (int tblidx = 0; tblidx < dataSet.Tables.Count; ++tblidx)
			{
				// Retrieve next available filename from queue (if any available):
				var filename = exportFilenames.Count > 0 ? exportFilenames.Dequeue().Trim() : null;

				// If there are no rows in this table and we are suppressing empty datatables, continue to next
				// table (doesn't matter whether we have a valid filename or not in this case)
				if (dataSet.Tables[tblidx].Rows.Count == 0 && suppressIfEmpty)
				{
					logger.LogInformation($"File [{filename ?? "(null)"}] not produced ({dataSet.Tables[tblidx].TableName} is empty, suppressed)");
				}
				// If filename is NULL (as opposed to blank), we have more datatables than filenames - throw exception
				// unless we have already exported at least one datatable and are ignoring unexpected tables:
				else if (filename == null)
				{
					if (tblidx > 0 && ignoreUnexpectedTables)
					{
						logger.LogInformation($"Discarded {dataSet.Tables[tblidx].Rows.Count} row(s) from {dataSet.Tables[tblidx].TableName} (unexpected, no filename)");
					}
					else
					{
						throw new Exception($"Unable to export {dataSet.Tables[tblidx].TableName} (no filename available)");
					}
				}
				// If filename is provided but blank, we are discarding this datatable:
				else if (string.IsNullOrEmpty(filename))
				{
					logger.LogInformation($"Discarded {dataSet.Tables[tblidx].Rows.Count} row(s) from {dataSet.Tables[tblidx].TableName} (blank filename)");
				}
				// Otherwise, we have data to potentially export and a filename to write to:
				else
				{
					var boundColumns = new List<OutputColumnBound>();
					string exportFilePath = Path.Combine(exportFolder, filename);

					// Check for an explicit column listing for this table:
					var outputColumnList = outputColumnListSet?.GetOutputColumnList(tblidx);
					if (outputColumnList?.Count == 0)
					{
						// Explicit column listing found, but no columns included (i.e. all were explicitly excluded from output), discard:
						logger.LogInformation($"Discarded {dataSet.Tables[tblidx].Rows.Count} row(s) from {dataSet.Tables[tblidx].TableName} (all columns excluded by config)");
					}
					else if (outputColumnList != null)
					{
						#region Construct bound output column collection from explicit column listing
						var validationerrors = new List<Exception>();
						for (int ocolidx = 0; ocolidx < outputColumnList.Count; ++ocolidx)
						{
							// Ensure datatable contains a column with this name:
							if (!dataSet.Tables[tblidx].Columns.Contains(outputColumnList[ocolidx].ColumnName))
							{
								validationerrors.Add(new ArgumentException($"Column {outputColumnList[ocolidx].ColumnName} does not exist in {dataSet.Tables[tblidx].TableName}"));
								continue; // Skip logic below, we will be throwing anyway (but want to catch any other column validation errors first)
							}

							// Add column to bound collection for use in data rendering logic below:
							var column = dataSet.Tables[tblidx].Columns[outputColumnList[ocolidx].ColumnName];
							boundColumns.Add(new OutputColumnBound
							{
								ColumnName = string.IsNullOrEmpty(outputColumnList[ocolidx].OutputNameOverride) ? column.ColumnName : outputColumnList[ocolidx].OutputNameOverride,
								Ordinal = column.Ordinal,
								ColumnType = column.DataType,
								ProviderType = column.ExtendedProperties["ProviderType"] as Type,
								FormatMethod = outputColumnList[ocolidx].FormatMethod,
								FormatString = outputColumnList[ocolidx].FormatString
							});
						}
						// If any columns failed to validate above, throw exception now:
						if (validationerrors.Count > 0)
						{
							throw new AggregateException(validationerrors);
						}
						#endregion
					}
					else
					{
						#region Construct bound column collection from datatable metadata
						for (int dcolidx = 0; dcolidx < dataSet.Tables[tblidx].Columns.Count; ++dcolidx)
						{
							boundColumns.Add(new OutputColumnBound
							{
								ColumnName = dataSet.Tables[tblidx].Columns[dcolidx].ColumnName,
								Ordinal = dcolidx,
								ColumnType = dataSet.Tables[tblidx].Columns[dcolidx].DataType,
								ProviderType = dataSet.Tables[tblidx].Columns[dcolidx].ExtendedProperties["ProviderType"] as Type,
								FormatMethod = Format.Auto
							});
						}
						#endregion
					}

					#region Create file and write table contents
					try
					{
						if (string.IsNullOrEmpty(exportPGPPublicKeyRing))
						{
							// Open destination file and streamwriter, write table contents
							using var filestream = new FileStream(exportFilePath, FileMode.Create, FileAccess.Write, FileShare.None);
							using var streamwriter = new StreamWriter(filestream);
							await WriteTable(streamwriter, dataSet.Tables[tblidx], boundColumns);
						}
						else
						{
							// PGP encryption required - open key file first (if this fails, we don't want to create output file):
							using var publickeystream = new FileStream(exportPGPPublicKeyRing, FileMode.Open, FileAccess.Read, FileShare.Read);
							// Open output file (automatically adding pgp extension, if required):
							if (!Path.GetExtension(exportFilePath).Equals("pgp", StringComparison.OrdinalIgnoreCase))
							{
								exportFilePath += ".pgp";
							}
							using var filestream = new FileStream(exportFilePath, FileMode.Create, FileAccess.Write, FileShare.None);

							// Create encryption stream around output file, then StreamWriter around top stream in encryption stack:
							using var encryptionstream = exportPGPRawFormat ? TaskUtilities.Pgp.GetEncryptionStreamRaw(publickeystream, exportPGPUserID, filestream)
								: TaskUtilities.Pgp.GetEncryptionStream(publickeystream, exportPGPUserID, filestream);
							using var streamwriter = new StreamWriter(encryptionstream.GetStream());

							// Write file contents (StreamWriter will write through encryption stream stack to output file stream):
							await WriteTable(streamwriter, dataSet.Tables[tblidx], boundColumns);
						}
					}
					catch (Exception ex)
					{
						throw new Exception($"Error writing {dataSet.Tables[tblidx].TableName} to {exportFilePath}",
							ex is AggregateException ae ? TaskUtilities.General.SimplifyAggregateException(ae) : ex);
					}
					#endregion

					logger.LogInformation($"Wrote {dataSet.Tables[tblidx].Rows.Count} rows from {dataSet.Tables[tblidx].TableName} to {exportFilePath}");
				}
			}
			#endregion

			// In theory, number of filenames should exactly equal number of datasets, log warning if names left over:
			if (exportFilenames.Count > 0)
			{
				var unusedFilenames = exportFilenames.Count(filename => !string.IsNullOrEmpty(filename));
				if (unusedFilenames > 0)
				{
					logger.LogWarning($"{unusedFilenames} unused filename(s) discarded");
				}
			}

			// Return output collection from export procedure, to be added to task return values:
			return outputParameters;
		}

		/// <summary>
		/// Perform actual writing of file contents, given DataTable and list of bound output columns
		/// </summary>
		private async Task WriteTable(StreamWriter streamwriter, DataTable datatable, List<OutputColumnBound> boundColumns)
		{
			int rowidx = -1, colidx = -1;
			try
			{
				// Write header row, if required:
				if (!suppressHeaders)
				{
					for (colidx = 0; colidx < boundColumns.Count; ++colidx)
					{
						if (colidx > 0) await streamwriter.WriteAsync(delimiter);
						await streamwriter.WriteAsync(boundColumns[colidx].ColumnName.Replace(delimiter, string.Empty));
					}
					await streamwriter.WriteLineAsync();
				}

				// Write all data rows:
				for (rowidx = 0; rowidx < datatable.Rows.Count; ++rowidx)
				{
					for (colidx = 0; colidx < boundColumns.Count; ++colidx)
					{
						if (colidx > 0) await streamwriter.WriteAsync(delimiter);
						await streamwriter.WriteAsync(ColumnValueToString(datatable.Rows[rowidx][boundColumns[colidx].Ordinal], boundColumns[colidx]));
					}
					await streamwriter.WriteLineAsync();
				}
			}
			catch (Exception ex)
			{
				throw new Exception(rowidx >= 0 ? $"Exception at row {rowidx} column {colidx}" : $"Exception at header row column {colidx}",
					ex is AggregateException ae ? TaskUtilities.General.SimplifyAggregateException(ae) : ex);
			}
		}

		/// <summary>
		/// Convert column value object (from DataRow) into string, using formatting specified by bound output column
		/// </summary>
		private string ColumnValueToString(object value, OutputColumnBound column)
		{
			if (value is byte[] byteval)
			{
				// Special handling required for binary/varbinary fields - convert to hex first:
				var converted = BitConverter.ToString(byteval).Replace("-", string.Empty);
				return column.FormatMethod switch
				{
					Format.Explicit => string.Format(column.FormatString, converted),
					Format.Auto => "0x" + converted,
					_ => converted
				};
			}
			else if (column.FormatMethod == Format.Explicit)
			{
				// Let string.Format handle any type-specific logic, but then check whether delimiter is present
				// in result (for example, DateTime formats may inadvertently introduce commas):
				var strval = string.Format(column.FormatString, value);
				if (strval.Contains(delimiter))
				{
					// If string was already qualified by FormatString, we do not need to take further action;
					// if it was not, we will need to either qualify or remove all delimiter instances:
					if (!qualifiedStringRegex.IsMatch(strval))
					{
						return qualifyStrings ? "\"" + quoteEscapeRegex.Replace(strval, "\"\"") + "\""
							: strval.Replace(delimiter, string.Empty);
					}
				}
				return strval;
			}
			else if (column.FormatMethod == Format.Raw)
			{
				// No formatting required, convert directly to string - note this will not check whether
				// delimiters are present in string, Raw format means trust exactly what comes from database
				return value is string strval ? strval : value.ToString();
			}

			// (Note from this point onward, FormatMethod is assumed to be Auto)

			else if (column.ProviderType == typeof(SqlMoney) && value is decimal decval) // Format SqlMoney fields as currency
			{
				return decval.ToString("C");
			}
			else if (value is string strval) // Special handling for string values
			{
				if (qualifiedStringRegex.IsMatch(strval)) // If string is already qualified, trust source value:
				{
					return strval;
				}
				else if (qualifyStrings) // Wrap string in double-quotes (any existing quotes must be escaped)
				{
					return "\"" + quoteEscapeRegex.Replace(strval, "\"\"") + "\"";
				}
				else // Output string as-is (any instances of delimiter will be removed)
				{
					return strval.Replace(delimiter, string.Empty);
				}
			}
			else // Default case - no formatting required, just convert to string directly:
			{
				return value.ToString();
			}
		}
		#endregion

		#region Private class definitions - Output column control
		public enum Format
		{
			Exclude,	// Exclude this column from output
			Auto,		// Dynamically format columns into strings based on column type
			Raw,		// Dump column values directly to file using ToString
			Explicit	// Use string.Format(FormatString) or [cell].ToString(FormatString) to generate output
		};

		/// <summary>
		/// Configuration template for container class detailing explicit output behavior for a
		/// single column from result set
		/// </summary>
		private class OutputColumnTemplate
		{
			#region Fields
			/// <summary>
			/// Name of column in dataset output by stored procedure
			/// </summary>
			public string ColumnName { get; init; }
			/// <summary>
			/// Optional replacement value for column header in output file
			/// </summary>
			public string OutputNameOverride { get; init; } = null;
			/// <summary>
			/// Method of formatting data to file
			/// </summary>
			public Format FormatMethod { get; init; } = Format.Auto;
			/// <summary>
			/// Format string for Explicit formatting method
			/// </summary>
			public string FormatString { get; init; } = null;
			#endregion

			#region Methods
			public bool IsInvalid()
			{
				// ColumnName is always required (to match up with dataset column name):
				if (string.IsNullOrEmpty(ColumnName))
				{
					return true;
				}
				// If formatting is being done explicitly, format string is required:
				else if (FormatMethod == Format.Explicit)
				{
					if (string.IsNullOrEmpty(FormatString)) return true;
					else if (!stringFormatRegex.IsMatch(FormatString)) return true;
				}
				// Otherwise, column is valid output:
				return false;
			}
			#endregion
		}

		/// <summary>
		/// Collection class for a set of OutputColumnTemplate lists (each corresponding to a
		/// result set from export stored procedure), or single shared list (for all result sets)
		/// </summary>
		private class OutputColumnTemplateListSet
		{
			#region Fields
			/// <summary>
			/// List of OutputColumnTemplate lists (each entry in outer list is for a specific
			/// result set, each entry in inner list is for a column within that result set)
			/// </summary>
			public List<List<OutputColumnTemplate>> ColumnListSet { get; init; } = null;
			#endregion

			#region Methods
			/// <summary>
			/// Retrieve column listing for the dataset at the specified ordinal
			/// </summary>
			public List<OutputColumnTemplate> GetOutputColumnList(int dataset)
			{
				// If no column data has been loaded, throw exception:
				if (ColumnListSet.Count == 0)
				{
					throw new InvalidOperationException($"Column list requested for dataset {dataset}, none available");
				}
				// If there is only one column data list available, the same output configuration
				// will be shared for all output files from this export:
				else if (ColumnListSet.Count == 1)
				{
					return ColumnListSet[0];
				}

				// Otherwise, return requested column configuration set (so long as it is within range):
				return dataset < ColumnListSet.Count ? ColumnListSet[dataset]
					: throw new InvalidOperationException($"Column list requested for dataset {dataset}, only {ColumnListSet.Count - 1} available");
			}
			/// <summary>
			/// Ensure that at least one output column list (with at least one valid output column) has been
			/// loaded, and that there are no invalid entries in any loaded list
			/// </summary>
			/// <returns></returns>
			public OutputColumnTemplateListSet EnsureValid()
			{
				// If column lists have not been loaded, throw exception:
				if ((ColumnListSet?.Count ?? 0) == 0)
				{
					throw new ArgumentException("Output column configuration is present, but no column data loaded");
				}

				var validationerrors = new List<Exception>();
				bool columnsfound = false;

				// Iterate through column lists, removing excluded columns (which may have been included in
				// configuration to prevent IConfiguration from ignoring them entirely) and validating:
				for (int i = 0; i < ColumnListSet.Count; ++i)
				{
					ColumnListSet[i].RemoveAll(column => column.FormatMethod == Format.Exclude);
					if (ColumnListSet[i].Count > 0)
					{
						columnsfound = true; // Flag that there is at least one non-empty list
						if (ColumnListSet[i].Any(column => column.IsInvalid()))
						{
							validationerrors.Add(new Exception($"Column list for dataset {i} includes invalid entries"));
						}
					}
				}

				// Ensure at least one of the loaded lists include columns:
				if (!columnsfound)
				{
					validationerrors.Add(new ArgumentException("Output column configuration is present, but no column lists include output columns"));
				}

				// If exceptions were added to collection, throw now (otherwise assume we are good):
				return validationerrors.Count > 0 ? throw new AggregateException(validationerrors) : this;
			}
			#endregion
		}

		/// <summary>
		/// Container class for explicit output behavior of the column at the specified Ordinal (built
		/// from metadata of actual result set, combined with OutputColumnTemplate if present)
		/// </summary>
		private class OutputColumnBound
		{
			#region Fields
			public string ColumnName { get; init; }
			public int Ordinal { get; init; }
			public Type ColumnType { get; init; }
			public Type ProviderType { get; init; }
			public Format FormatMethod { get; init; }
			public string FormatString { get; init; }
			#endregion
		}
		#endregion

		#region Private class definitions - Misc
		/// <summary>
		/// Container class for a file ready to be exported, as returned by queue check procedure
		/// </summary>
		private class QueuedFile
		{
			/// <summary>
			/// FileID value for this specific file (if NULL, default/parameter value will be used)
			/// </summary>
			public int? FileID { get; init; } = null;
			/// <summary>
			/// Optional Subfolder (under configured destination folder) for file output
			/// </summary>
			public string Subfolder { get; init; } = null;
		}
		#endregion
	}
}

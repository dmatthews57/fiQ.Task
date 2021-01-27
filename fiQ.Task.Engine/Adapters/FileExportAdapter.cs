using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using fiQ.Task.Models;
using fiQ.Task.Utilities;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.Task.Adapters
{
	public class FileExportAdapter : TaskAdapter
	{
		#region Fields and constructors
		private string exportProcedureName = null;		// Stored procedure for actual data export
		private int? exportProcedureTimeout = null;     // Optional timeout (in seconds) for each export procedure call
		private bool defaultNulls = false;				// If true, explicit DBNull value will be passed to all unrecognized stored procedure inputs
		private bool suppressIfEmpty = false;			// If true, files with no content will not be produced (and empty datasets will not generate errors)
		private bool suppressHeaders = false;           // If true, output files will not include CSV headers
		private string delimiter = null;				// Used when outputting multiple columns (default comma)
		private Dictionary<string, string> sqlParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		private OutputColumnListSet outputColumnListSet = null;

		public FileExportAdapter(IConfiguration _config, ILogger<FileExportAdapter> _logger, string taskName = null)
			: base(_config, _logger, taskName) { }
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
				exportProcedureName = parameters.GetString("ExportProcedureName");
				string exportFolder = parameters.GetString("ExportFolder", TaskUtilities.REGEX_DIRPATH, dateTimeNow);
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
				bool haltOnExportError = parameters.GetBool("HaltOnExportError");
				defaultNulls = parameters.GetBool("DefaultNulls");
				suppressIfEmpty = parameters.GetBool("SuppressIfEmpty");
				suppressHeaders = parameters.GetBool("SuppressHeaders");
				delimiter = parameters.GetString("Delimiter");
				if (string.IsNullOrEmpty(delimiter))
				{
					delimiter = ",";
				}

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
					outputColumnListSet = outputColumnConfig.Get<OutputColumnListSet>().EnsureValid();
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
										logger.LogWarning("Discarded row from dataset (null FileID)");
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
							throw new AlreadyHandledException();
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
								ex = TaskUtilities.SimplifyAggregateException(ae);
							}

							// Log error here (to take advantage of logger scope context) and add exception to result collection:
							logger.LogError(ex, "Error exporting file");
							result.AddException(new Exception($"Error exporting file{(queuedFile.FileID == null ? string.Empty : $" (FileID {queuedFile.FileID}")}", ex));

							// If we are halting process for individual export errors, throw custom exception to indicate
							// to outer try block that it can just exit without re-logging:
							if (haltOnExportError)
							{
								throw new AlreadyHandledException();
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

					// If this point is reached, consider overall operation successful
					result.Success = true;
				}
			}
			catch (AlreadyHandledException)
			{
				// Exception was handled above - just proceed with return below
			}
			catch (Exception ex)
			{
				if (ex is AggregateException ae) // Exception caught from async task; simplify if possible
				{
					ex = TaskUtilities.SimplifyAggregateException(ae);
				}
				logger.LogError(ex, "File export failed");
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
			SqlCommandBuilder.DeriveParameters(cmd); // Note: synchronous (no async version available)
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
						do
						{
							#region Construct DataTable column collection from reader schema
							var dataTable = new DataTable();
							var schemaTable = await dr.GetSchemaTableAsync();
							foreach (DataRow schemarow in schemaTable.Rows)
							{
								dataTable.Columns.Add(new DataColumn((string)schemarow["ColumnName"], (Type)schemarow["DataType"]));
							}
							#endregion

							#region Read all rows from this result set into DataTable and add to DataSet
							var values = new object[dataTable.Columns.Count];
							while (await dr.ReadAsync())
							{
								dr.GetValues(values);
								dataTable.Rows.Add(values);
							}
							dataSet.Tables.Add(dataTable);
							#endregion
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

			#region Iterate through all datatables in dataset, creating and writing contents to files
			for (int i = 0; i < dataSet.Tables.Count; ++i)
			{
				// Retrieve next available filename from queue (if any available):
				var filename = exportFilenames.Count > 0 ? exportFilenames.Dequeue() : null;

				// If there are no rows in this table and we are suppressing empty datatables, continue to next
				// table (doesn't matter whether we have a valid filename or not in this case)
				if (dataSet.Tables[i].Rows.Count == 0 && suppressIfEmpty)
				{
					logger.LogInformation($"File [{filename ?? "(null)"}] not produced (datatable {i} is empty, suppressed)");
				}
				// If filename is NULL (as opposed to blank), we have more datatables than filenames - throw exception:
				else if (filename == null)
				{
					throw new Exception($"Unable to export datatable {i} (no filename available)");
				}
				// If filename is provided but blank, we are discarding this datatable:
				else if (string.IsNullOrEmpty(filename))
				{
					logger.LogInformation($"Discarded {dataSet.Tables[i].Rows.Count} rows from datatable {i} (blank filename)");
				}
				// Otherwise, we have data to potentially export and a filename to write to:
				else
				{
					// Check for an explicit column listing for this table:
					var outputColumnList = outputColumnListSet?.GetOutputColumnList(i);
					if (outputColumnList != null)
					{
						// Explicit column listing found - if there are no columns included (i.e. all were explicitly excluded
						// from output), discard this datatable:
						if (outputColumnList.Count == 0)
						{
							logger.LogInformation($"Discarded {dataSet.Tables[i].Rows.Count} rows from datatable {i} (all columns excluded by config)");
						}
						else // Otherwise, write datatable to target file with explicit formatting:
						{
							await WriteTableExplicit(dataSet.Tables[i], outputColumnList, Path.Combine(exportFolder, filename));
						}
					}
					else // No explicit column listing, write datatable to target file dynamically:
					{
						await WriteTableDynamic(dataSet.Tables[i], Path.Combine(exportFolder, filename));
					}
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
		/// Write data table contents into target file, dynamically determining columns
		/// </summary>
		private async System.Threading.Tasks.Task WriteTableDynamic(DataTable dataTable, string exportFilePath)
		{
			logger.LogInformation($"Exporting {dataTable.Rows.Count} rows to {exportFilePath}");
		}
		/// <summary>
		/// Write data table contents into target file, binding column values as indicated by explicit column list
		/// </summary>
		private async System.Threading.Tasks.Task WriteTableExplicit(DataTable dataTable, List<OutputColumn> outputColumnList, string exportFilePath)
		{
			logger.LogInformation($"Exporting {dataTable.Rows.Count} rows to {exportFilePath} with {outputColumnList.Count} explicit columns");

			using var outputstream = Console.OpenStandardOutput();
			using var streamwriter = new StreamWriter(outputstream);
			var quoteregex = new Regex("\\\\?[\"]"); // Detect double-quote, optionally preceded by backslash

			#region Write file header
			if (!suppressHeaders)
			{
				for (int i = 0; i < outputColumnList.Count; ++i)
				{
					if (i > 0) await streamwriter.WriteAsync(delimiter);
					string columnName = string.IsNullOrEmpty(outputColumnList[i].OutputNameOverride) ? outputColumnList[i].ColumnName : outputColumnList[i].OutputNameOverride;
					await streamwriter.WriteAsync(columnName.IndexOf(delimiter) >= 0 ? $"\"{quoteregex.Replace(columnName, "\\\"")}\"" : columnName);
				}
				await streamwriter.WriteLineAsync();
			}
			#endregion

			// TODO: CONTINUE ON TO DATA...
		}
		#endregion

		#region Private class definitions
		/// <summary>
		/// Container class for explicit output behavior for a single column from result set
		/// </summary>
		private class OutputColumn
		{
			#region Fields and enums
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

			public enum Format
			{
				Exclude,	// Exclude this column from output
				Auto,       // Dynamically format columns into strings based on column type
				Raw,        // Dump column values directly to file using ToString
				Explicit    // Use string.Format with FormatString to generate output
			};
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
				else if (FormatMethod == Format.Explicit && string.IsNullOrEmpty(FormatString))
				{
					return true;
				}
				// Otherwise, column is valid output:
				return false;
			}
			#endregion
		}

		/// <summary>
		/// Collection class for a set of OutputColumn lists (each corresponding to a result set
		/// from export stored procedure), or single shared list (for all result sets)
		/// </summary>
		private class OutputColumnListSet
		{
			#region Fields
			/// <summary>
			/// List of ColumnSet lists
			/// </summary>
			public List<List<OutputColumn>> ColumnListSet { get; init; } = null;
			#endregion

			#region Methods
			/// <summary>
			/// Retrieve column listing for the dataset at the specified ordinal
			/// </summary>
			public List<OutputColumn> GetOutputColumnList(int dataset)
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
			public OutputColumnListSet EnsureValid()
			{
				// If column lists have not been loaded, throw exception:
				if ((ColumnListSet?.Count ?? 0) == 0)
				{
					throw new ArgumentException("Output column configuration is present, but no column data loaded");
				}

				// Iterate through column lists, removing excluded columns (which may have been included in
				// configuration to prevent IConfiguration from ignoring them entirely) and validating:
				foreach (var columnList in ColumnListSet)
				{
					columnList.RemoveAll(column => column.FormatMethod == OutputColumn.Format.Exclude);
					if (columnList.Any(column => column.IsInvalid()))
					{
						throw new InvalidDataException("One or more output column lists include invalid entries");
					}
				}

				// If none of the loaded column lists include any output columns, throw exception:
				if (!ColumnListSet.Any(columnlist => columnlist.Count > 0))
				{
					throw new ArgumentException("Output column configuration is present, but no column lists include output columns");
				}

				// Otherwise, assume we are good:
				return this;
			}
			#endregion
		}

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

		/// <summary>
		/// Custom exception class to indicate to ExecuteTask that it can immediately exit function without
		/// needing to execute any further handler logic (logging, adding to result collection, etc.)
		/// </summary>
		private class AlreadyHandledException : Exception
		{
			public AlreadyHandledException()
			{
			}
			public AlreadyHandledException(string message)
				: base(message)
			{
			}
			public AlreadyHandledException(string message, Exception inner)
				: base(message, inner)
			{
			}
		}
		#endregion
	}
}

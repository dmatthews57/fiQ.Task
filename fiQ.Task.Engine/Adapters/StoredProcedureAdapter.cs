using System;
using System.Data;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using fiQ.TaskModels;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.TaskAdapters
{
	/// <summary>
	/// TaskAdapter to execute a specific SQL stored procedure
	/// </summary>
	public class StoredProcedureAdapter : TaskAdapter
	{
		#region Fields and constructors
		public StoredProcedureAdapter(IConfiguration _config, ILogger<StoredProcedureAdapter> _logger, string taskName = null)
			: base(_config, _logger, taskName) { }
		#endregion

		/// <summary>
		/// Execute the specified SQL stored procedure (deriving parameters automatically, applying inputs from
		/// parameters and adding output parameters to return values collection)
		/// </summary>
		public override async Task<TaskResult> ExecuteTask(TaskParameters parameters)
		{
			var result = new TaskResult();
			var consoleOutput = new StringBuilder();
			var dateTimeNow = DateTime.Now; // Use single value throughout for consistency in macro replacements
			try
			{
				#region Retrieve task parameters
				// Retrieve and validate required parameters
				string connectionString = config.GetConnectionString(parameters.GetString("ConnectionString"));
				string storedProcedureName = parameters.GetString("StoredProcedureName");
				if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(storedProcedureName))
				{
					throw new ArgumentException("Missing or invalid ConnectionString and/or StoredProcedureName");
				}

				// Retrieve optional parameters
				string returnValueRegex = parameters.GetString("ReturnValueRegex");
				bool defaultNulls = parameters.GetBool("DefaultNulls");
				var dbTimeout = parameters.Get<int>("DBTimeout", int.TryParse);
				#endregion

				#region Open database connection and execute procedure
				await using (var cnn = new SqlConnection(connectionString))
				{
					await cnn.OpenAsync();
					// Capture console messages into StringBuilder:
					cnn.InfoMessage += (object obj, SqlInfoMessageEventArgs e) => { consoleOutput.AppendLine(e.Message); };

					await using (var cmd = new SqlCommand(storedProcedureName, cnn) { CommandType = CommandType.StoredProcedure })
					{
						if (dbTimeout > 0) cmd.CommandTimeout = (int)dbTimeout;

						#region Derive and apply procedure parameters
						SqlCommandBuilder.DeriveParameters(cmd); // Note: synchronous (no async version available)
						foreach (SqlParameter sqlParameter in cmd.Parameters)
						{
							if (sqlParameter.Direction.HasFlag(ParameterDirection.Input))
							{
								// This parameter requires input value - check for corresponding parameter:
								if (parameters.ContainsKey(sqlParameter.ParameterName))
								{
									// Special case - strings are not automatically changed to Guid values, attempt explicit conversion:
									if (sqlParameter.SqlDbType == SqlDbType.UniqueIdentifier)
									{
										sqlParameter.Value = (object)parameters.Get<Guid>(sqlParameter.ParameterName, Guid.TryParse) ?? DBNull.Value;
									}
									else
									{
										// Apply string value to parameter (replacing null with DBNull):
										sqlParameter.Value = (object)parameters.GetString(sqlParameter.ParameterName, null, dateTimeNow) ?? DBNull.Value;
									}
								}
								// If parameter value was not set above, and either we are set to supply default NULL
								// values OR this is also an OUTPUT parameter, set to explicit NULL:
								else if (defaultNulls || sqlParameter.Direction.HasFlag(ParameterDirection.Output))
								{
									sqlParameter.Value = DBNull.Value;
								}
								// (otherwise, value will be left unspecified; if stored procedure does not provide a default
								// value, execution will fail and missing parameter will be indicated by exception string)
							}
						}
						#endregion

						await cmd.ExecuteNonQueryAsync();

						#region Extract output parameters into return values collection, validate return value
						foreach (SqlParameter sqlParameter in cmd.Parameters)
						{
							if (sqlParameter.Direction.HasFlag(ParameterDirection.Output))
							{
								result.AddReturnValue(sqlParameter.ParameterName, sqlParameter.Value == DBNull.Value ? null : sqlParameter.Value?.ToString());
							}
						}

						// If return value regex provided, check return value against it:
						if (!string.IsNullOrEmpty(returnValueRegex))
						{
							string returnValue = result.ReturnValues.ContainsKey("@RETURN_VALUE") ? result.ReturnValues["@RETURN_VALUE"] : null;
							if (string.IsNullOrEmpty(returnValue))
							{
								throw new Exception($"Stored procedure did not return a value (or no value retrieved)");
							}
							else if (!Regex.IsMatch(returnValue, returnValueRegex))
							{
								throw new Exception($"Invalid stored procedure return value ({returnValue})");
							}
						}
						#endregion

						// If this point is reached with no exception raised, operation was successful:
						result.Success = true;
					}
				}
				#endregion
			}
			catch (Exception ex)
			{
				if (ex is AggregateException ae) // Exception caught from async task; simplify if possible
				{
					ex = TaskUtilities.General.SimplifyAggregateException(ae);
				}
				logger.LogError(ex, "Procedure execution failed");
				result.AddException(ex);
			}

			// If console output was captured, log now:
			if (consoleOutput.Length > 0)
			{
				logger.LogDebug(consoleOutput.ToString());
			}
			return result;
		}
	}
}

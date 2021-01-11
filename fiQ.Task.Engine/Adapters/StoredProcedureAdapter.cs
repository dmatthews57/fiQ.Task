using System;
using System.Data;
using System.Data.SqlClient;
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
	/// <summary>
	/// TaskAdapter to execute a specific SQL stored procedure
	/// </summary>
	public class StoredProcedureAdapter : TaskAdapter
	{
		public StoredProcedureAdapter(IConfiguration _config, ILogger<DirectoryCleanerAdapter> _logger, string taskName = null)
			: base(_config, _logger, taskName) { }

		/// <summary>
		/// Check whether specified file(s) exist and set result
		/// </summary>
		public override async Task<TaskResult> ExecuteTask(TaskParameters parameters)
		{
			var result = new TaskResult();
			var consoleOutput = new StringBuilder();
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
				using (var cnn = new SqlConnection(connectionString))
				{
					cnn.Open();
					// Capture console messages into StringBuilder:
					cnn.InfoMessage += (object obj, SqlInfoMessageEventArgs e) => { consoleOutput.AppendLine(e.Message); };

					using (var cmd = new SqlCommand(storedProcedureName, cnn) { CommandType = CommandType.StoredProcedure })
					{
						if (dbTimeout > 0) cmd.CommandTimeout = (int)dbTimeout;

						#region Derive and apply procedure parameters
						SqlCommandBuilder.DeriveParameters(cmd);
						foreach (SqlParameter sqlParameter in cmd.Parameters)
						{
							if (sqlParameter.Direction.HasFlag(ParameterDirection.Input))
							{
								// This parameter requires input value - check for corresponding parameter:
								bool needsDefault = true;
								if (parameters.ContainsKey(sqlParameter.ParameterName))
								{
									// Special case - strings are not automatically changed to Guid values:
									if (sqlParameter.SqlDbType == SqlDbType.UniqueIdentifier)
									{
										// Attempt explicit conversion of parameter to Guid value:
										var guid = parameters.Get<Guid>(sqlParameter.ParameterName, Guid.TryParse);
										if (guid != null)
										{
											sqlParameter.Value = guid;
											needsDefault = false;
										}
									}
									else
									{
										// Apply string value to parameter:
										sqlParameter.Value = parameters.GetString(sqlParameter.ParameterName, null, DateTime.Now);
										needsDefault = false;
									}
								}

								// If parameter value was not set above, and either we are set to supply default NULL
								// values OR this is also an OUTPUT parameter, set to explicit NULL:
								if (needsDefault && (defaultNulls || sqlParameter.Direction.HasFlag(ParameterDirection.Output)))
								{
									sqlParameter.Value = DBNull.Value;
								}

								// (if value was not set, it will be left unspecified; if stored procedure does not provide a
								// default value, execution will fail and this will be indicated by exception string)
							}
						}
						#endregion

						cmd.ExecuteNonQuery();

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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using fiQ.TaskAdapters;
using fiQ.TaskModels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace fiQ
{
	/// <summary>
	/// Class responsible for creating and executing TaskAdapters based on incoming configuration
	/// </summary>
	public class TaskEngine : IDisposable
	{
		#region Fields and constructors
		private readonly IServiceProvider isp = null;	// For providing dependency injection support to created TaskAdapters
		private readonly ILogger logger = null;
		private IDisposable loggerScope = null;	// Optional: logging scope for overall job name
		private bool disposed = false;

		public TaskEngine(IServiceProvider _isp, ILogger<TaskEngine> _logger, string jobName = null)
		{
			isp = _isp;
			logger = _logger;
			if (!string.IsNullOrEmpty(jobName))
			{
				loggerScope = logger.BeginScope(new Dictionary<string, object>() { ["JobName"] = jobName });
			}
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
			if (disposed == false && disposing && loggerScope != null)
			{
				loggerScope.Dispose();
				loggerScope = null;
			}
			disposed = true;
		}
		#endregion

		public async Task<TaskSetResult> Execute(List<TaskParameters> tasks, TaskEngineConfig engineConfig)
		{
			var tasksetresult = new TaskSetResult();

			#region Iterate through all tasks in collection
			foreach (var task in tasks)
			{
				try
				{
					// Create TaskAdapter instance based on configuration object:
					using (var t = LoadAdapter(task))
					{
						// Merge return values from previous step(s) into parameter set (overwriting configuration parameters, if required):
						task.MergeParameters(tasksetresult.ReturnValues);

						#region Execute TaskAdapter and handle result
						var result = await t.ExecuteTask(task);
						if (result.Success == false || result.Exceptions.Any()) // Execution failed, or succeeded with errors
						{
							// Generate logging data for caller:
							tasksetresult.LogMessage.AppendLine($"Task [{task.TaskName}] {(result.Success ? "succeeded with errors" : "execution failed")}");
							foreach (var ex in result.Exceptions)
							{
								tasksetresult.LogMessage.AppendLine(ex.ToString());
							}

							// Update return value, unless it is already set to a higher (i.e. worse) value:
							if (tasksetresult.ReturnValue < (result.Success ? 7 : 8))
							{
								tasksetresult.ReturnValue = (result.Success ? 7 : 8);
							}
						}
						else // Execution succeeded
						{
							tasksetresult.LogMessage.AppendLine($"Task [{task.TaskName}] executed successfully");
							if (tasksetresult.ReturnValue < 0)
							{
								tasksetresult.ReturnValue = 0;
							}
							// Merge any return values output by TaskAdapter into overall set result:
							tasksetresult.MergeReturnValues(result.ReturnValues);
						}
						#endregion
					}
				}
				catch (LoadAdapterException ex)
				{
					// Exception loading adapter object; if value returned is higher (i.e. worse) than current return value, update:
					if (tasksetresult.ReturnValue < ex.ErrorCode)
					{
						tasksetresult.ReturnValue = ex.ErrorCode;
					}
					// Add details to logging output:
					tasksetresult.LogMessage.AppendLine($"Task [{task.TaskName}] failed adapter creation: {ex}");
				}
				catch (Exception ex) // General exception (likely during TaskAdapter execution):
				{
					if (tasksetresult.ReturnValue < 9)
					{
						tasksetresult.ReturnValue = 9;
					}
					if (ex is AggregateException ae) // Exception caught from async task; simplify if possible
					{
						ex = TaskUtilities.General.SimplifyAggregateException(ae);
					}
					// Add details to logging output:
					tasksetresult.LogMessage.AppendLine($"Task [{task.TaskName}] execution error: {ex}");
				}

				// If this step was not successful and caller requested halting batch when error encountered, stop now:
				if (tasksetresult.ReturnValue != 0 && engineConfig.HaltOnError)
				{
					tasksetresult.LogMessage.AppendLine("Halting task list due to error executing previous step");
					break;
				}
			}
			#endregion

			#region If return value debugging requested, log final set of return values
			if (engineConfig.DebugReturnValues)
			{
				foreach (var returnvalue in tasksetresult.ReturnValues)
				{
					logger.LogDebug("RETURN VALUE {@returnvalue}", new { key = returnvalue.Key, value = returnvalue.Value });
				}
			}
			#endregion

			return tasksetresult;
		}

		#region Private methods
		/// <summary>
		/// Create an instance of the specified TaskAdapter class from class name, DLL and path
		/// </summary>
		/// <param name="task">Configuration object for requested TaskAdapter</param>
		private TaskAdapter LoadAdapter(TaskParameters task)
		{
			// Ensure caller has provided name of class to load:
			if (string.IsNullOrEmpty(task.AdapterClassName))
			{
				throw new LoadAdapterException(10, "TaskAdapter class name not provided");
			}

			try
			{
				// Load Assembly from DLL name (if provided - otherwise look in "this" Assembly):
				var assembly = string.IsNullOrEmpty(task.AdapterDLLName) ? Assembly.GetExecutingAssembly()
					: Assembly.LoadFrom(Path.Combine(string.IsNullOrEmpty(task.AdapterDLLPath) ? AppDomain.CurrentDomain.BaseDirectory : task.AdapterDLLPath, task.AdapterDLLName));
				if (assembly == null)
				{
					throw BuildLoadAdapterException(11, new ArgumentException("Invalid DLL path/name, or assembly not loaded"), task);
				}

				// Retrieve collection of types from assembly with a base type of TaskAdapter:
				var assemblyTypes = assembly.GetTypes()?.Where(t => t.IsSubclassOf(typeof(TaskAdapter)));
				if (assemblyTypes?.Any() ?? throw BuildLoadAdapterException(12, new ArgumentException("Assembly contains no TaskAdapters"), task))
				{
					// Locate assembly with matching name (short or fully-qualified):
					foreach (var type in assemblyTypes)
					{
						if (type.FullName.Equals(task.AdapterClassName, StringComparison.OrdinalIgnoreCase)
							|| type.Name.Equals(task.AdapterClassName, StringComparison.OrdinalIgnoreCase))
						{
							// Create TaskAdapter instance (ActivatorUtilities will handle dependency injection, so long as TaskName
							// (TaskAdapter constructor argument) is not null (which would confuse activator):
							return (TaskAdapter)ActivatorUtilities.CreateInstance(isp, type, task.TaskName ?? string.Empty);
						}
					}
				}

				// If this point is reached, class was not found:
				throw BuildLoadAdapterException(13, new ArgumentException("Specified class not found in assembly"), task);
			}
			catch (LoadAdapterException)
			{
				throw;
			}
			catch (Exception ex)
			{
				throw BuildLoadAdapterException(14, ex, task);
			}
		}

		/// <summary>
		/// Construct a LoadAdapterException with provided return value and nested exception
		/// </summary>
		private static LoadAdapterException BuildLoadAdapterException(int errorCode, Exception ex, TaskParameters task)
		{
			return new LoadAdapterException(
				errorCode,
				// Format outer exception error string combining arguments to allow client to see what was attempted:
				string.Concat("Error initializing adapter ",
					string.IsNullOrEmpty(task.AdapterDLLName) ? string.Empty : (string.IsNullOrEmpty(task.AdapterDLLPath) ? task.AdapterDLLName : Path.Combine(task.AdapterDLLPath, task.AdapterDLLName)),
					"::",
					task.AdapterClassName),
				ex);
		}
		#endregion

		#region Private class definitions
		private class LoadAdapterException : Exception
		{
			public int ErrorCode { get; init; }
			public LoadAdapterException(int errorCode, string message) : base(message) => ErrorCode = errorCode;
			public LoadAdapterException(int errorCode, string message, Exception inner) : base(message, inner) => ErrorCode = errorCode;
		}
		#endregion
	}
}

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Reflection;
using fiQ.Task.Adapters;
using fiQ.Task.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace fiQ.Task.Engine
{
	/// <summary>
	/// Class responsible for creating and executing TaskAdapters based on incoming configuration
	/// </summary>
	class TaskEngine : IDisposable
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
		public void Dispose() => Dispose(true);
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

		public int Execute(IOrderedDictionary adapters, bool haltOnError, TaskParameters overrideParameters, out string logMessage)
		{
			foreach (DictionaryEntry adapter in adapters)
			{
				var config = adapter.Value as TaskAdapterConfig;
				using (var t = GetTaskAdapter(isp, config.AdapterClassName, config.AdapterDLLName, config.AdapterDLLPath, adapter.Key))
				{
					var taskParameters = new TaskParameters(config.TaskParameters);
					if (overrideParameters?.Parameters?.Count > 0)
					{
						taskParameters.MergeParameters(overrideParameters.Parameters);
					}
					var result = t.ExecuteTask(taskParameters);
					overrideParameters.MergeParameters(result.ReturnValues);
				}
			}
			logMessage = $"Running {adapters.Count} adapters, {(haltOnError ? "halting" : "not halting")} on errors, {overrideParameters.Parameters.Count} override parameters";
			return 9;
		}

		#region Private methods
		/// <summary>
		/// Create an instance of the specified TaskExecutor class from the specified DLL and path
		/// </summary>
		/// <param name="serviceProvider">Required from Program.Main, for dependency injection</param>
		/// <param name="adapterClassName">Name of class to instantiate</param>
		/// <param name="adapterDLLName">Name of DLL in which class is located (optional: if not provided, looks in current assembly)</param>
		/// <param name="adapterDLLPath">Path to specified DLL (optional: if not provided, looks in current folder)</param>
		/// <param name="ctorparameters">Constructor arguments to be passed to created object</param>
		/// <returns></returns>
		private static TaskAdapter GetTaskAdapter(IServiceProvider serviceProvider,
			string adapterClassName, string adapterDLLName, string adapterDLLPath,
			params object[] ctorparameters)
		{
			try
			{
				// Load Assembly from DLL name (if provided - otherwise look in "this" Assembly):
				var assembly = string.IsNullOrEmpty(adapterDLLName) ? Assembly.GetExecutingAssembly()
					: Assembly.LoadFrom(Path.Combine(string.IsNullOrEmpty(adapterDLLPath) ? AppDomain.CurrentDomain.BaseDirectory : adapterDLLPath, adapterDLLName));
				if (assembly == null)
				{
					throw new ArgumentException("Invalid DLL path/name, or assembly not loaded");
				}

				// Retrieve collection of types from assembly with a base type of TaskAdapter:
				var assemblyTypes = assembly.GetTypes()?.Where(t => t.IsSubclassOf(typeof(TaskAdapter)));
				if (assemblyTypes?.Any() ?? throw new ArgumentException("Assembly contains no TaskAdapters"))
				{
					// Locate assembly with matching name (short or fully-qualified):
					foreach (var type in assemblyTypes)
					{
						if (type.FullName.Equals(adapterClassName, StringComparison.OrdinalIgnoreCase)
							|| type.Name.Equals(adapterClassName, StringComparison.OrdinalIgnoreCase))
						{
							return (TaskAdapter)ActivatorUtilities.CreateInstance(serviceProvider, type, ctorparameters);
						}
					}
				}
				throw new ArgumentException("Specified class not found in assembly");
			}
			catch (Exception ex)
			{
				// Format arguments into exception, to allow client to see what was attempted here
				throw new Exception(string.Concat("Error initializing adapter ",
					string.IsNullOrEmpty(adapterDLLName) ? string.Empty : (string.IsNullOrEmpty(adapterDLLPath) ? adapterDLLName : Path.Combine(adapterDLLPath, adapterDLLName)),
					"::", adapterClassName), ex);
			}
		}
		#endregion
	}
}

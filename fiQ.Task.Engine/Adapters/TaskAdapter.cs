using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using fiQ.Task.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.Task.Adapters
{
	/// <summary>
	/// Base class for TaskAdapter (class which will perform execution of a particular task step)
	/// </summary>
	public abstract class TaskAdapter : IDisposable
	{
		#region Fields and constructors
		protected readonly IConfiguration config;
		protected readonly ILogger logger;
		private IDisposable loggerScope = null;
		private bool disposed = false;

		protected TaskAdapter(
			IConfiguration _config,
			ILogger<TaskAdapter> _logger,
			string taskName = null)
		{
			config = _config;
			logger = _logger;

			// If task name provided, create logging scope
			if (!string.IsNullOrEmpty(taskName))
			{
				loggerScope = logger.BeginScope(new Dictionary<string, object>() { ["TaskName"] = Path.GetFileName(taskName) });
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

		/// <summary>
		/// Definition of abstract async execution function for this task adapter
		/// </summary>
		/// <param name="parameters">Configuration object for this task</param>
		/// <returns>Task wrapping a TaskResult object</returns>
		public abstract Task<TaskResult> ExecuteTask(ITaskParameters parameters);
	}
}

using System;
using System.Collections.Generic;
using System.IO;
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
			string jobName = null,
			string taskName = null)
		{
			config = _config;
			logger = _logger;

			// If job or task names provided, create logging scope
			var scope = new Dictionary<string, object>();
			if (!string.IsNullOrEmpty(jobName))
			{
				scope["JobName"] = jobName;
			}
			if (!string.IsNullOrEmpty(taskName))
			{
				scope["TaskName"] = Path.GetFileName(taskName);
			}
			if (scope.Count > 0)
			{
				loggerScope = logger.BeginScope(scope);
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

		public abstract TaskResult ExecuteTask(TaskParameters parameters);
	}
}

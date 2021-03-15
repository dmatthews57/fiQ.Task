using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace fiQ.TaskUtilities
{
	/// <summary>
	/// Helper class to allow adding incremental values to logging scope
	/// </summary>
	public class LogScopeHelper : IDisposable
	{
		#region Fields and constructors
		private ILogger logger;
		private Dictionary<string, object> state;
		private IDisposable loggerScope = null;
		private bool disposed = false;

		public LogScopeHelper(ILogger _logger, Dictionary<string, object> _state = null)
		{
			logger = _logger;
			state = _state;
			if (state?.Count > 0)
			{
				loggerScope = logger.BeginScope(state);
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
			if (disposed == false && disposing)
			{
				if (loggerScope != null)
				{
					loggerScope.Dispose();
					loggerScope = null;
				}
			}
			disposed = true;
		}
		#endregion

		#region Methods
		/// <summary>
		/// Add entry to logger state dictionary and update scope
		/// </summary>
		public void AddToState(string key, string value)
		{
			state[key] = value;

			// Dispose of existing scope, if any:
			if (loggerScope != null)
			{
				loggerScope.Dispose();
				loggerScope = null;
			}

			// Begin new logging scope with updated state:
			loggerScope = logger.BeginScope(state);
		}
		#endregion
	}
}

using System;

namespace fiQ.TaskModels
{
	/// <summary>
	/// Custom exception class to indicate to ExecuteTask that it can immediately exit function without
	/// needing to execute any further handler logic (logging, adding to result collection, etc.)
	/// </summary>
	internal class TaskExitException : Exception
	{
		public TaskExitException()
		{
		}
		public TaskExitException(string message)
			: base(message)
		{
		}
		public TaskExitException(string message, Exception inner)
			: base(message, inner)
		{
		}
	}
}

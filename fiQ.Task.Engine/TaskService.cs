using System;
using fiQ.Task.Models;
using fiQ.Task.Utilities;
using Microsoft.Extensions.Logging;

namespace fiQ.Task.Engine
{
	class TaskService
	{
		IServiceProvider isp = null;
		ILogger logger = null;

		public TaskService(IServiceProvider _isp, ILogger<TaskService> _logger)
		{
			isp = _isp;
			logger = _logger;
		}

		public void DoSomething(string jobName, string taskDirName, string taskFileName, bool haltOnError, TaskParameters overrideParameters)
		{
			if (!string.IsNullOrEmpty(taskDirName))
			{
				logger.LogInformation($"Doing folder {taskDirName}");
			}
			else if (!string.IsNullOrEmpty(taskFileName))
			{
				logger.LogInformation($"Doing file {taskFileName}");
			}
			else
			{
				logger.LogError("Exiting disappointedly");
			}

			using (var t = TaskUtilities.GetTaskAdapter(isp, "DirectoryCleanerAdapter", null, null, jobName ?? string.Empty, taskFileName ?? string.Empty))
			{
				var taskParameters = new TaskParameters();
				taskParameters.MergeParameters(overrideParameters.Parameters);
				var result = t.ExecuteTask(taskParameters);
			}
		}
	}
}

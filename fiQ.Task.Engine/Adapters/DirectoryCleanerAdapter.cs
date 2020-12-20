using fiQ.Task.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.Task.Adapters
{
	class DirectoryCleanerAdapter : TaskAdapter
	{
		public DirectoryCleanerAdapter(IConfiguration _config, ILogger<DirectoryCleanerAdapter> _logger, string taskName = null)
			: base(_config, _logger, taskName) { }

		public override TaskResult ExecuteTask(TaskParameters parameters)
		{
			//logger.LogWarning("Warning {connstr}", config.GetConnectionString("TESTDB"));
			//logger.LogDebug("Debug {connstr}", config.GetConnectionString("TESTDB2"));
			return new TaskResult
			{
				Success = false
			};
		}

	}
}

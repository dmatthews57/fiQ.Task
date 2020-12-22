using System.Threading.Tasks;
using fiQ.Task.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.Task.Adapters
{
	public class DirectoryCleanerAdapter : TaskAdapter
	{
		public DirectoryCleanerAdapter(IConfiguration _config, ILogger<DirectoryCleanerAdapter> _logger, string taskName = null)
			: base(_config, _logger, taskName) { }

		public override async Task<TaskResult> ExecuteTask(ITaskParameters parameters)
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

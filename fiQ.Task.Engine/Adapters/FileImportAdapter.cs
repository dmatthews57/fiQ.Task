using System;
using System.Threading.Tasks;
using fiQ.TaskModels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.TaskAdapters
{
	public class FileImportAdapter : TaskAdapter
	{
		#region Fields and constructors
		public FileImportAdapter(IConfiguration _config, ILogger<FileImportAdapter> _logger, string taskName = null)
			: base(_config, _logger, taskName) { }
		#endregion

		/// <summary>
		/// TODO: DESCRIPTION
		/// </summary>
		public override async Task<TaskResult> ExecuteTask(TaskParameters parameters)
		{
			var result = new TaskResult();
			try
			{
				#region Retrieve task parameters
				#endregion
			}
			catch (Exception ex)
			{
				logger.LogError(ex, "File import failed");
				result.AddException(ex);
			}
			return result;
		}
	}
}

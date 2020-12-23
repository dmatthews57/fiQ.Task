using System;
using System.Threading.Tasks;
using fiQ.Task.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.Task.Adapters
{
	/// <summary>
	/// TaskAdapter to set parameters for use by subsequent TaskAdapters in batch
	/// </summary>
	public class SetParmAdapter : TaskAdapter
	{
		public SetParmAdapter(IConfiguration _config, ILogger<DirectoryCleanerAdapter> _logger, string taskName = null)
			: base(_config, _logger, taskName) { }

		/// <summary>
		/// Apply all values received in parameters collection to ReturnValue collection
		/// </summary>
		public override async Task<TaskResult> ExecuteTask(TaskParameters parameters)
		{
			var result = new TaskResult();
			try
			{
				// Iterate through all keys in incoming parameter collection, and add to ReturnValues
				// collection (performing no Regex validation, but processing macros):
				foreach (var key in parameters.GetKeys())
				{
					result.AddReturnValue(key, parameters.GetString(key, null, true));
				}

				result.Success = true;
			}
			catch (Exception ex)
			{
				result.AddException(ex);
			}
			return result;
		}
	}
}

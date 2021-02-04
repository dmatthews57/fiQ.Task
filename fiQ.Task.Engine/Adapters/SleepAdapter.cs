using System;
using System.Threading.Tasks;
using fiQ.TaskModels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.TaskAdapters
{
	/// <summary>
	/// TaskAdapter to delay for a specific number of milliseconds
	/// </summary>
	public class SleepAdapter : TaskAdapter
	{
		#region Fields and constructors
		public SleepAdapter(IConfiguration _config, ILogger<SetParmAdapter> _logger, string taskName = null)
			: base(_config, _logger, taskName) { }
		#endregion

		/// <summary>
		/// Sleep for a specified number of milliseconds
		/// </summary>
		public override async Task<TaskResult> ExecuteTask(TaskParameters parameters)
		{
			var result = new TaskResult();
			try
			{
				// Read and validate millisecond delay value:
				var millisecondsDelay = parameters.Get<int>("MillisecondsDelay", int.TryParse) ?? 0;
				if (millisecondsDelay <= 0)
				{
					throw new ArgumentException("Missing or invalid MillisecondsDelay");
				}

				// Await specified delay, and return success:
				await Task.Delay(millisecondsDelay);
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

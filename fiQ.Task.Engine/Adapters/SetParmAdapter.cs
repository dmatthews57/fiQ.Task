﻿using System;
using System.Threading.Tasks;
using fiQ.TaskModels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.TaskAdapters
{
	/// <summary>
	/// TaskAdapter to set parameters for use by subsequent TaskAdapters in batch
	/// </summary>
	public class SetParmAdapter : TaskAdapter
	{
		#region Fields and constructors
		public SetParmAdapter(IConfiguration _config, ILogger<SetParmAdapter> _logger, string taskName = null)
			: base(_config, _logger, taskName) { }
		#endregion

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
				var dateTimeNow = DateTime.Now;
				foreach (var key in parameters.GetKeys())
				{
					result.AddReturnValue(key, parameters.GetString(key, null, dateTimeNow));
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

using System;
using System.Linq;
using System.Text.RegularExpressions;
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
			var dateTimeNow = DateTime.Now; // Use single value throughout for consistency in macro replacements
			try
			{
				// Iterate through all keys in incoming parameter collection:
				foreach (var key in parameters.GetKeys())
				{
					// Retrieve specified key value, processing date macros:
					var value = parameters.GetString(key, null, dateTimeNow);

					// Now process any nested macros in resulting value string - regex will capture argument-named macros,
					// to allow keys passed as parameters to this adapter (including the keys of return values output by
					// previous adapters in batch) to have their values updated with the value of other keys in collection
					// (again including return values output by previous steps). For example, if a previous adapter in this
					// batch output a return value of "@FileID"/57, placing the parameter "Command"/"echo <@@FileID>" in
					// the collection for THIS adapter will result in value "Command"/"echo 57" being placed in the return
					// value collection (and then placed into input parameter collections of subsequent steps):
					var valuemacromatches = TaskUtilities.General.REGEX_NESTEDPARM_MACRO
						.Matches(value)
						.Cast<Match>()
						// Flatten match collection into name/value pair and select unique values only:
						.Select(match => new { Name = match.Groups["name"].Value, Value = match.Value })
						.Distinct();
					foreach (var match in valuemacromatches)
					{
						// Retrieve parameter matching the "name" portion of the macro - processing date/time macros
						// again - and replace all instances of the specified macro with the string retrieved:
						value = value.Replace(match.Value, parameters.GetString(match.Name, null, dateTimeNow));
					}

					// Add final value to return value collection:
					result.AddReturnValue(key, value);
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

﻿using System;
using System.Collections.Generic;
using System.Text;

namespace fiQ.Task.Models
{
	/// <summary>
	/// Container class for return data from TaskEngine's Execute function (representing one or more
	/// executions of TaskAdapter::ExecuteTask functions)
	/// </summary>
	public class TaskSetResult
	{
		#region Fields
		private StringBuilder logMessageBuilder { get; set; } = new StringBuilder();
		private Dictionary<string, string> returnValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		#endregion

		#region Properties
		/// <summary>
		/// Integer return value for overall result of adapter executions
		/// </summary>
		public int ReturnValue { get; set; } = -1;
		/// <summary>
		/// Output of logging data assembled by TaskEngine
		/// </summary>
		public string LogMessage
		{
			get
			{
				return logMessageBuilder.ToString();
			}
		}
		/// <summary>
		/// Collection of output/return values (key/value pairs) generated by task execution
		/// </summary>
		public IReadOnlyDictionary<string, string> ReturnValues
		{
			get { return returnValues; }
		}
		#endregion

		#region Methods
		/// <summary>
		/// Add line to logging data
		/// </summary>
		public void AppendLogLine(string line)
		{
			logMessageBuilder.AppendLine(line);
		}
		/// <summary>
		/// Merge a collection of values into parameter set (overwriting, if necessary)
		/// </summary>
		public void MergeReturnValues(IReadOnlyDictionary<string, string> values)
		{
			foreach (var parm in values)
			{
				returnValues[parm.Key] = parm.Value;
			}
		}
		#endregion
	}
}

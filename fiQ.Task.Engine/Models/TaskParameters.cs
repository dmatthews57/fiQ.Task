using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using fiQ.Task.Utilities;

namespace fiQ.Task.Models
{
	/// <summary>
	/// Container class for parameters to be passed as input to a TaskAdapter's ExecuteTask function
	/// </summary>
	public class TaskParameters
	{
		#region Fields
		private Dictionary<string, string> parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		#endregion

		#region Properties
		public IReadOnlyDictionary<string, string> Parameters
		{
			get { return parameters; }
		}
		#endregion

		#region Public methods - configuration accessors
		/// <summary>
		///  Retrieve count of parameters in configuration collection
		/// </summary>
		public int Count()
		{
			return parameters.Count;
		}

		/// <summary>
		/// Retrieve collection of dictionary keys (to allow independent iteration through parameters)
		/// </summary>
		public IEnumerable<string> GetKeys()
		{
			return parameters.Keys;
		}

		/// <summary>
		/// Retrieve string value from parameter collection
		/// </summary>
		/// <param name="name">Name/dictionary key of parameter to retrieve</param>
		/// <param name="parmRegex">Optional Regex object to apply to parameter</param>
		/// <param name="processDateMacros">If set to true, date macros in value will be applied</param>
		/// <returns>Null if value does not match Regex, otherwise value (string.Empty if not found)</returns>
		public string GetString(string name, Regex parmRegex = null, bool processDateMacros = false)
		{
			if (parameters.ContainsKey(name))
			{
				string value = processDateMacros ? TaskUtilities.ApplyDateMacros(parameters[name]) : parameters[name];
				return (parmRegex == null) ? value : (parmRegex.IsMatch(value) ? value : null);
			}
			return string.Empty;
		}

		/// <summary>
		/// Retrieve boolean value from parameter collection
		/// </summary>
		/// <returns>True if value indicates, false otherwise (including if parameter not found)</returns>
		public bool GetBool(string name)
		{
			if (parameters.ContainsKey(name))
			{
				string value = parameters[name];
				if (!string.IsNullOrEmpty(value))
				{
					// Attempt to parse value string automatically:
					if (bool.TryParse(value, out var result))
					{
						return result;
					}
					// Otherwise check for other "true" values not supported by bool.Parse:
					else if (value.Equals("y", StringComparison.OrdinalIgnoreCase)
						|| value.Equals("yes", StringComparison.OrdinalIgnoreCase)
						|| value.Equals("1"))
					{
						return true;
					}
				}
			}
			// Default to false (if not configured, or not a "true" value):
			return false;
		}
		#endregion

		#region Public methods - set parameter values
		/// <summary>
		/// Add (or update) a specific parameter name/value pair
		/// </summary>
		public void AddParameter(string name, string value)
		{
			parameters[name] = value;
		}

		/// <summary>
		/// Merge a collection of values into parameter set (overwriting, if necessary)
		/// </summary>
		public void MergeParameters(IReadOnlyDictionary<string, string> values)
		{
			foreach (var parm in values)
			{
				parameters[parm.Key] = parm.Value;
			}
		}
		#endregion
	}
}

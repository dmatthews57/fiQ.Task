using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;

namespace fiQ.TaskModels
{
	/// <summary>
	/// Container class for configuring a specific TaskAdapter and set of execution parameters
	/// </summary>
	public class TaskParameters
	{
		#region Fields, constructors and delegates
		public string TaskName { get; init; }
		public string AdapterClassName { get; init; }
		public string AdapterDLLName { get; init; }
		public string AdapterDLLPath { get; init; }
		/// <summary>
		/// Optional configuration object containing extended configuration data
		/// </summary>
		/// <remarks>
		/// Allows TaskAdapter child classes access to strongly-typed custom configuration objects
		/// </remarks>
		public IConfiguration Configuration { get; init; } = null;
		/// <summary>
		/// Private name:value parameter collection (accessed via public methods only)
		/// </summary>
		private Dictionary<string, string> parameters { get; init; }
		public TaskParameters()
		{
			parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		}
		public TaskParameters(IReadOnlyDictionary<string, string> _parameters)
		{
			parameters = new Dictionary<string, string>(_parameters, StringComparer.OrdinalIgnoreCase);
		}
		/// <summary>
		/// Delegate for generic strongly-typed parameter parsing (requires a TryParse or equivalent)
		/// </summary>
		public delegate bool TryParseHandler<T>(string value, out T result);
		#endregion

		#region Public methods - configuration accessors
		/// <summary>
		/// Retrieve collection of dictionary keys (to allow independent iteration through parameters)
		/// </summary>
		public IEnumerable<string> GetKeys()
		{
			return parameters.Keys;
		}

		/// <summary>
		/// Check whether dictionary contains the specified key
		/// </summary>
		public bool ContainsKey(string name)
		{
			return parameters.ContainsKey(name);
		}

		/// <summary>
		/// Retrieve string value from parameter collection
		/// </summary>
		/// <param name="name">Name/dictionary key of parameter to retrieve</param>
		/// <param name="parmRegex">Optional Regex object to apply to parameter</param>
		/// <param name="processDateMacros">If set to true, date macros in value will be applied</param>
		/// <returns>Null if value does not match Regex, otherwise value (string.Empty if not found)</returns>
		public string GetString(string name, Regex parmRegex = null, DateTime? applyMacroTime = null)
		{
			if (parameters.ContainsKey(name))
			{
				string value = applyMacroTime == null ? parameters[name] : TaskUtilities.General.ApplyDateMacros(parameters[name], (DateTime)applyMacroTime);
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

		/// <summary>
		/// Retrieve strongly-typed value from parameter collection using specified conversion delegate
		/// </summary>
		public T? Get<T>(string name, TryParseHandler<T> handler) where T : struct
		{
			string value = parameters.ContainsKey(name) ? parameters[name] : null;
			if (!string.IsNullOrEmpty(value))
			{
				if (handler(value, out var result))
				{
					return result;
				}
			}
			return null;
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

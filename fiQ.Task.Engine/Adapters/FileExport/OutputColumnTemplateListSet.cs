using System;
using System.Collections.Generic;
using System.Linq;

namespace fiQ.TaskAdapters.FileExport
{
	/// <summary>
	/// Collection class for a set of OutputColumnTemplate lists (each corresponding to a result
	/// set from export stored procedure), or single shared list (for all result sets)
	/// </summary>
	internal class OutputColumnTemplateListSet
	{
		#region Fields
		/// <summary>
		/// List of OutputColumnTemplate lists (each entry in outer list is for a specific
		/// result set, each entry in inner list is for a column within that result set)
		/// </summary>
		public List<List<OutputColumnTemplate>> ColumnListSet { get; init; } = null;
		#endregion

		#region Methods
		/// <summary>
		/// Retrieve column listing for the dataset at the specified ordinal
		/// </summary>
		public List<OutputColumnTemplate> GetOutputColumnList(int dataset)
		{
			// If no column data has been loaded, throw exception:
			if (ColumnListSet.Count == 0)
			{
				throw new InvalidOperationException($"Column list requested for dataset {dataset}, none available");
			}
			// If there is only one column data list available, the same output configuration
			// will be shared for all output files from this export:
			else if (ColumnListSet.Count == 1)
			{
				return ColumnListSet[0];
			}

			// Otherwise, return requested column configuration set (so long as it is within range):
			return dataset < ColumnListSet.Count ? ColumnListSet[dataset]
				: throw new InvalidOperationException($"Column list requested for dataset {dataset}, only {ColumnListSet.Count - 1} available");
		}

		/// <summary>
		/// Ensure that at least one output column list (with at least one valid output column) has been
		/// loaded, and that there are no invalid entries in any loaded list
		/// </summary>
		/// <returns></returns>
		public OutputColumnTemplateListSet EnsureValid()
		{
			// If column lists have not been loaded, throw exception:
			if ((ColumnListSet?.Count ?? 0) == 0)
			{
				throw new ArgumentException("Output column configuration is present, but no column data loaded");
			}

			var validationerrors = new List<Exception>();
			bool columnsfound = false;

			// Iterate through column lists, removing excluded columns (which may have been included in
			// configuration to prevent IConfiguration from ignoring them entirely) and validating:
			for (int i = 0; i < ColumnListSet.Count; ++i)
			{
				ColumnListSet[i].RemoveAll(column => column.FormatMethod == Format.Exclude);
				if (ColumnListSet[i].Count > 0)
				{
					columnsfound = true; // Flag that there is at least one non-empty list
					if (ColumnListSet[i].Any(column => column.IsInvalid()))
					{
						validationerrors.Add(new Exception($"Column list for dataset {i} includes invalid entries"));
					}
				}
			}

			// Ensure at least one of the loaded lists include columns:
			if (!columnsfound)
			{
				validationerrors.Add(new ArgumentException("Output column configuration is present, but no column lists include output columns"));
			}

			// If exceptions were added to collection, throw now (otherwise assume we are good):
			return validationerrors.Count > 0 ? throw new AggregateException(validationerrors) : this;
		}
		#endregion
	}
}

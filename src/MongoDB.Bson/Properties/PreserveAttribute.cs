﻿/* Copyright 2020-present MongoDB Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.ComponentModel;

namespace MongoDB.Bson.Internals
{
	/// <summary>
	/// Prevents the Xamarin managed linker from linking the target.
	/// </summary>
	[AttributeUsage(AttributeTargets.All)]
	[EditorBrowsable(EditorBrowsableState.Never)]
	public sealed class PreserveAttribute : Attribute
	{
		/// <summary>
		/// When used on a class rather than a property, ensures that all members of this type are preserved.
		/// </summary>
		public bool AllMembers { get; set; }

		/// <summary>
		/// Flags the method as a method to preserve during linking if the container class is pulled in.
		/// </summary>
		public bool Conditional { get; set; }
	}
}

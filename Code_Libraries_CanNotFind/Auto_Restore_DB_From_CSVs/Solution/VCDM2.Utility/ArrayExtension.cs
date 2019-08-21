using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace VCDM2.Utility
{
	public static class ArrayExtension
	{
		public static T[] SubArrayDeepClone<T>(this T[] data, int index, int length)
		{
			T[] arrCopy = new T[length];
			Array.Copy(data, index, arrCopy, 0, length);
			using (MemoryStream ms = new MemoryStream())
			{
				var bf = new BinaryFormatter();
				bf.Serialize(ms, arrCopy);
				ms.Position = 0;
				return (T[])bf.Deserialize(ms);
			}
		}
	}
}

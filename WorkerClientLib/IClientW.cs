using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkerClientLib
{
    public interface IClientW
    {
        byte[] GetSplit(long start, long end);
        void ReturnResult(IList<KeyValuePair<string, string>> result, int split);
    }
}

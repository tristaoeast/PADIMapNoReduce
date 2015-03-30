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
        File GetSplit(int start, int end);
        void ReturnResult(IList<KeyValuePair<string, string>> result);
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MapLib
{
    public interface IMap
    {
        IList<KeyValuePair<String, String>> Map(String fileLine);
    }
}

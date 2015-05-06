using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PADIMapNoReduceLibs
{
    public interface IPuppetMaster
    {
        String Worker(String id, String serviceUrl, String entryUrl);
    }
}

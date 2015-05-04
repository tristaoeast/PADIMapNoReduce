using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JobTrackerWorkerLib
{
    public interface IWorkerJT
    {
        void SubmitJobToWorker(long start, long end, int split);
        bool SendMapper(String className, byte[] code);
    }
}

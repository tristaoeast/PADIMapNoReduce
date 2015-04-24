using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JobTrackerWorkerLib
{
    public interface IWorkerJT
    {
        void SubmitJobToWorker();
        bool SendMapper(String className, byte[] code);
    }
}

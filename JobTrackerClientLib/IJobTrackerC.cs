using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JobTrackerClientLib
{
    public interface IJobTrackerC
    {
        void submitJob(long fileSize, int splits);
    }
}

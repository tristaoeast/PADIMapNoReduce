﻿using MapLib;using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PADIMapNoReduceLibs
{
    public interface IClient
    {
        void Submit(String inputFile, int splits, String outputDirectory, String mapClassName, string dllPath);
        void Init(String entryUrl);
        byte[] GetSplit(long start, long end);
        void ReturnResult(IList<KeyValuePair<string, string>> result, int split);
        void notifyJobFinished(bool finished);
    }
}

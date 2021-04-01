# SwissLog
The source code of ISSRE'20 conference paper [SwissLog](https://ieeexplore.ieee.org/abstract/document/9251078/). We have submitted an extended version to a journal, and the source code here is the extended version. 

The file structure is as belows:
```
SwissLog
├─anomaly_detection
│  └─encoder
├─offline_logparser
│  ├─evaluator
│  ├─layers
│  └─utils
└─online_logparser
    ├─evaluator
    ├─layers
    └─utils
```


## Requirements
python 3.7
```
pip install -r requirements.txt
```

## Reference

```
@inproceedings{li2020swisslog,
  title={SwissLog: Robust and Unified Deep Learning Based Log Anomaly Detection for Diverse Faults},
  author={Li, Xiaoyun and Chen, Pengfei and Jing, Linxiao and He, Zilong and Yu, Guangba},
  booktitle={2020 IEEE 31st International Symposium on Software Reliability Engineering (ISSRE)},
  pages={92--103},
  year={2020},
  organization={IEEE}
}
```


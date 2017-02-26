package com.kakao.cuesheet.examples

import com.kakao.cuesheet.CueSheet

object SimpleHiveExample extends CueSheet {{
  hiveContext.sql("show databases").show(truncate = false)
}}

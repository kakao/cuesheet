package com.kakao.cuesheet.examples

import com.kakao.cuesheet.CueSheet

/** You can access command line argument in the CueSheet body */
object ArgsExample extends CueSheet {{
  println(s"There are total ${args.length} args")
  args.foreach(println)
}}

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files.{readAllLines, write}
import java.nio.file.Paths

import sbt.State
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.ReleasePlugin.autoImport._

import scala.collection.JavaConversions._
import scala.sys.process._

object UpdateReadme {

  /** update the library version in README.md before the release commit */
  def updateReadme(state: State): State = {
    val revision = "\"([^\"]+)\"".r.findFirstMatchIn(readAllLines(Paths.get("version.sbt"), UTF_8).mkString("")).get.group(1)

    val lines = readAllLines(Paths.get("README.md"), UTF_8)

    val start = lines.indexWhere(_.startsWith("<!-- DO NOT EDIT: The section below"))
    val end = lines.indexWhere(_.startsWith("<!-- DO NOT EDIT: The section above"))

    if (start == -1 || end == -1) {
      throw new RuntimeException("Could not find markers on README.md")
    }

    val before = lines.take(start + 2)
    val after = lines.takeRight(lines.size - end + 1)

    System.setProperty("com.kakao.cuesheet.configuration", "")
    write(Paths.get("README.md"), before ++ Seq(
      s"""libraryDependencies += "com.kakao.cuesheet" %% "cuesheet" % "$revision""""
    ) ++ after, UTF_8)

    "git add README.md"!

    state
  }

  /** insert updateReadme action in the release process */
  val customReleaseProcess: Seq[ReleaseStep] = Seq(
    checkSnapshotDependencies, inquireVersions, runTest, setReleaseVersion,
    ReleaseStep(action = updateReadme),
    commitReleaseVersion, tagRelease, publishArtifacts,
    setNextVersion, commitNextVersion, pushChanges
  )

}

package gitbucket.core.util

import java.io.{OutputStream, ByteArrayOutputStream}

import gitbucket.core.service.RepositoryService
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.diff.{RawText, DiffEntry, DiffFormatter}
import org.eclipse.jgit.util.io.NullOutputStream
import Directory._
import StringUtil._
import ControlUtil._
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import org.eclipse.jgit.lib._
import org.eclipse.jgit.revwalk._
import org.eclipse.jgit.revwalk.filter._
import org.eclipse.jgit.treewalk._
import org.eclipse.jgit.treewalk.filter._
import org.eclipse.jgit.diff.DiffEntry.ChangeType
import org.eclipse.jgit.errors.{ConfigInvalidException, MissingObjectException}
import org.eclipse.jgit.transport.RefSpec
import java.util.Date
import org.eclipse.jgit.api.errors.{JGitInternalException, InvalidRefNameException, RefAlreadyExistsException, NoHeadException}
import org.eclipse.jgit.dircache.DirCacheEntry
import org.slf4j.LoggerFactory

//import difflib._

import scala.collection.mutable.ArrayBuffer

/**
 * Provides complex JGit operations.
 */
object JGitUtil {

  private val logger = LoggerFactory.getLogger(JGitUtil.getClass)

  /**
   * The repository data.
   *
   * @param owner the user name of the repository owner
   * @param name the repository name
   * @param url the repository URL
   * @param commitCount the commit count. If the repository has over 1000 commits then this property is 1001.
   * @param branchList the list of branch names
   * @param tags the list of tags
   */
  case class RepositoryInfo(owner: String, name: String, url: String, commitCount: Int, branchList: List[String], tags: List[TagInfo]){
    def this(owner: String, name: String, baseUrl: String) = {
      this(owner, name, s"${baseUrl}/git/${owner}/${name}.git", 0, Nil, Nil)
    }
  }

  /**
   * The file data for the file list of the repository viewer.
   *
   * @param id the object id
   * @param isDirectory whether is it directory
   * @param name the file (or directory) name
   * @param message the last commit message
   * @param commitId the last commit id
   * @param time the last modified time
   * @param author the last committer name
   * @param mailAddress the committer's mail address
   * @param linkUrl the url of submodule
   */
  case class FileInfo(id: ObjectId, isDirectory: Boolean, name: String, message: String, commitId: String,
                      time: Date, author: String, mailAddress: String, linkUrl: Option[String])

  /**
   * The commit data.
   *
   * @param id the commit id
   * @param shortMessage the short message
   * @param fullMessage the full message
   * @param parents the list of parent commit id
   * @param authorTime the author time
   * @param authorName the author name
   * @param authorEmailAddress the mail address of the author
   * @param commitTime the commit time
   * @param committerName  the committer name
   * @param committerEmailAddress the mail address of the committer
   */
  case class CommitInfo(id: String, shortMessage: String, fullMessage: String, parents: List[String],
                        authorTime: Date, authorName: String, authorEmailAddress: String,
                        commitTime: Date, committerName: String, committerEmailAddress: String){
    
    def this(rev: org.eclipse.jgit.revwalk.RevCommit) = this(
        rev.getName,
        rev.getShortMessage,
        rev.getFullMessage,
        rev.getParents().map(_.name).toList,
        rev.getAuthorIdent.getWhen,
        rev.getAuthorIdent.getName,
        rev.getAuthorIdent.getEmailAddress,
        rev.getCommitterIdent.getWhen,
        rev.getCommitterIdent.getName,
        rev.getCommitterIdent.getEmailAddress)

    val summary = getSummaryMessage(fullMessage, shortMessage)

    val description = defining(fullMessage.trim.indexOf("\n")){ i =>
      if(i >= 0){
        Some(fullMessage.trim.substring(i).trim)
      } else None
    }

    def isDifferentFromAuthor: Boolean = authorName != committerName || authorEmailAddress != committerEmailAddress
  }

//  case class DiffInfo(changeType: ChangeType, oldPath: String, newPath: String, oldContent: Option[String], newContent: Option[String])

  case class DiffInfoExtend(changeType: ChangeType, oldPath: String, newPath: String, diffContent: Option[List[DiffRow]])

  /**
   * The file content data for the file content view of the repository viewer.
   *
   * @param viewType "image", "large" or "other"
   * @param content the string content
   * @param charset the character encoding
   */
  case class ContentInfo(viewType: String, content: Option[String], charset: Option[String]){
    /**
     * the line separator of this content ("LF" or "CRLF")
     */
    val lineSeparator: String = if(content.exists(_.indexOf("\r\n") >= 0)) "CRLF" else "LF"
  }

  /**
   * The tag data.
   *
   * @param name the tag name
   * @param time the tagged date
   * @param id the commit id
   */
  case class TagInfo(name: String, time: Date, id: String)

  /**
   * The submodule data
   *
   * @param name the module name
   * @param path the path in the repository
   * @param url the repository url of this module
   */
  case class SubmoduleInfo(name: String, path: String, url: String)

  case class BranchMergeInfo(ahead: Int, behind: Int, isMerged: Boolean)

  case class BranchInfo(name: String, committerName: String, commitTime: Date, committerEmailAddress:String, mergeInfo: Option[BranchMergeInfo], commitId: String)

  /**
   * Returns RevCommit from the commit or tag id.
   * 
   * @param git the Git object
   * @param objectId the ObjectId of the commit or tag
   * @return the RevCommit for the specified commit or tag
   */
  def getRevCommitFromId(git: Git, objectId: ObjectId): RevCommit = {
    val revWalk = new RevWalk(git.getRepository)
    val revCommit = revWalk.parseAny(objectId) match {
      case r: RevTag => revWalk.parseCommit(r.getObject)
      case _         => revWalk.parseCommit(objectId)
    }
    revWalk.dispose
    revCommit
  }
  
  /**
   * Returns the repository information. It contains branch names and tag names.
   */
  def getRepositoryInfo(owner: String, repository: String, baseUrl: String): RepositoryInfo = {
    using(Git.open(getRepositoryDir(owner, repository))){ git =>
      try {
        // get commit count
        val commitCount = git.log.all.call.iterator.asScala.map(_ => 1).take(10001).sum

        RepositoryInfo(
          owner, repository, s"${baseUrl}/git/${owner}/${repository}.git",
          // commit count
          commitCount,
          // branches
          git.branchList.call.asScala.map { ref =>
            ref.getName.stripPrefix("refs/heads/")
          }.toList,
          // tags
          git.tagList.call.asScala.map { ref =>
            val revCommit = getRevCommitFromId(git, ref.getObjectId)
            TagInfo(ref.getName.stripPrefix("refs/tags/"), revCommit.getCommitterIdent.getWhen, revCommit.getName)
          }.toList
        )
      } catch {
        // not initialized
        case e: NoHeadException => RepositoryInfo(
          owner, repository, s"${baseUrl}/git/${owner}/${repository}.git", 0, Nil, Nil)

      }
    }
  }

  /**
   * Returns the file list of the specified path.
   * 
   * @param git the Git object
   * @param revision the branch name or commit id
   * @param path the directory path (optional)
   * @return HTML of the file list
   */
  def getFileList(git: Git, revision: String, path: String = "."): List[FileInfo] = {
    var list = new scala.collection.mutable.ListBuffer[(ObjectId, FileMode, String, String, Option[String])]

    using(new RevWalk(git.getRepository)){ revWalk =>
      val objectId  = git.getRepository.resolve(revision)
      val revCommit = revWalk.parseCommit(objectId)

      val treeWalk = if (path == ".") {
        val treeWalk = new TreeWalk(git.getRepository)
        treeWalk.addTree(revCommit.getTree)
        treeWalk
      } else {
        val treeWalk = TreeWalk.forPath(git.getRepository, path, revCommit.getTree)
        treeWalk.enterSubtree()
        treeWalk
      }

      using(treeWalk) { treeWalk =>
        while (treeWalk.next()) {
          // submodule
          val linkUrl = if(treeWalk.getFileMode(0) == FileMode.GITLINK){
            getSubmodules(git, revCommit.getTree).find(_.path == treeWalk.getPathString).map(_.url)
          } else None

          list.append((treeWalk.getObjectId(0), treeWalk.getFileMode(0), treeWalk.getPathString, treeWalk.getNameString, linkUrl))
        }

        list.transform(tuple =>
          if (tuple._2 != FileMode.TREE)
            tuple
          else
            simplifyPath(tuple)
        )

        @tailrec
        def simplifyPath(tuple: (ObjectId, FileMode, String, String, Option[String])): (ObjectId, FileMode, String, String, Option[String]) = {
          val list = new scala.collection.mutable.ListBuffer[(ObjectId, FileMode, String, String, Option[String])]
          using(new TreeWalk(git.getRepository)) { walk =>
            walk.addTree(tuple._1)
            while (walk.next() && list.size < 2) {
              val linkUrl = if (walk.getFileMode(0) == FileMode.GITLINK) {
                getSubmodules(git, revCommit.getTree).find(_.path == walk.getPathString).map(_.url)
              } else None
              list.append((walk.getObjectId(0), walk.getFileMode(0), tuple._3 + "/" + walk.getPathString, tuple._4 + "/" + walk.getNameString, linkUrl))
            }
          }
          if (list.size != 1 || list.exists(_._2 != FileMode.TREE))
            tuple
          else
            simplifyPath(list(0))
        }
      }
    }

    val commits = getLatestCommitFromPaths(git, list.toList.map(_._3), revision)
    list.map { case (objectId, fileMode, path, name, linkUrl) =>
      defining(commits(path)){ commit =>
        FileInfo(
          objectId,
          fileMode == FileMode.TREE || fileMode == FileMode.GITLINK,
          name,
          getSummaryMessage(commit.getFullMessage, commit.getShortMessage),
          commit.getName,
          commit.getAuthorIdent.getWhen,
          commit.getAuthorIdent.getName,
          commit.getAuthorIdent.getEmailAddress,
          linkUrl)
      }
    }.sortWith { (file1, file2) =>
      (file1.isDirectory, file2.isDirectory) match {
        case (true , false) => true
        case (false, true ) => false
       case _ => file1.name.compareTo(file2.name) < 0
      }
    }.toList
  }

  /**
   * Returns the first line of the commit message.
   */
  private def getSummaryMessage(fullMessage: String, shortMessage: String): String = {
    defining(fullMessage.trim.indexOf("\n")){ i =>
      defining(if(i >= 0) fullMessage.trim.substring(0, i).trim else fullMessage){ firstLine =>
        if(firstLine.length > shortMessage.length) shortMessage else firstLine
      }
    }
  }

  /**
   * Returns the commit list of the specified branch.
   * 
   * @param git the Git object
   * @param revision the branch name or commit id
   * @param page the page number (1-)
   * @param limit the number of commit info per page. 0 (default) means unlimited.
   * @param path filters by this path. default is no filter.
   * @return a tuple of the commit list and whether has next, or the error message
   */
  def getCommitLog(git: Git, revision: String, page: Int = 1, limit: Int = 0, path: String = ""): Either[String, (List[CommitInfo], Boolean)] = {
    val fixedPage = if(page <= 0) 1 else page
    
    @scala.annotation.tailrec
    def getCommitLog(i: java.util.Iterator[RevCommit], count: Int, logs: List[CommitInfo]): (List[CommitInfo], Boolean)  =
      i.hasNext match {
        case true if(limit <= 0 || logs.size < limit) => {
          val commit = i.next
          getCommitLog(i, count + 1, if(limit <= 0 || (fixedPage - 1) * limit <= count) logs :+ new CommitInfo(commit) else logs)
        }
        case _ => (logs, i.hasNext)
      }
    
    using(new RevWalk(git.getRepository)){ revWalk =>
      defining(git.getRepository.resolve(revision)){ objectId =>
        if(objectId == null){
          Left(s"${revision} can't be resolved.")
        } else {
          revWalk.markStart(revWalk.parseCommit(objectId))
          if(path.nonEmpty){
            revWalk.setRevFilter(new RevFilter(){
              def include(walk: RevWalk, commit: RevCommit): Boolean = {
                getDiffs(git, commit.getName, false, false)._1.find(_.newPath == path).nonEmpty
              }
              override def clone(): RevFilter = this
            })
          }
          Right(getCommitLog(revWalk.iterator, 0, Nil))
        }
      }
    }
  }

  def getCommitLogs(git: Git, begin: String, includesLastCommit: Boolean = false)
                   (endCondition: RevCommit => Boolean): List[CommitInfo] = {
    @scala.annotation.tailrec
    def getCommitLog(i: java.util.Iterator[RevCommit], logs: List[CommitInfo]): List[CommitInfo] =
      i.hasNext match {
        case true  => {
          val revCommit = i.next
          if(endCondition(revCommit)){
            if(includesLastCommit) logs :+ new CommitInfo(revCommit) else logs
          } else {
            getCommitLog(i, logs :+ new CommitInfo(revCommit))
          }
        }
        case false => logs
      }

    using(new RevWalk(git.getRepository)){ revWalk =>
      revWalk.markStart(revWalk.parseCommit(git.getRepository.resolve(begin)))
      getCommitLog(revWalk.iterator, Nil).reverse
    }
  }

  
  /**
   * Returns the commit list between two revisions.
   * 
   * @param git the Git object
   * @param from the from revision
   * @param to the to revision
   * @return the commit list
   */
  // TODO swap parameters 'from' and 'to'!?
  def getCommitLog(git: Git, from: String, to: String): List[CommitInfo] =
    getCommitLogs(git, to)(_.getName == from)
  
  /**
   * Returns the latest RevCommit of the specified path.
   * 
   * @param git the Git object
   * @param path the path
   * @param revision the branch name or commit id
   * @return the latest commit
   */
  def getLatestCommitFromPath(git: Git, path: String, revision: String): Option[RevCommit] =
    getLatestCommitFromPaths(git, List(path), revision).get(path)

  /**
   * Returns the list of latest RevCommit of the specified paths.
   *
   * @param git the Git object
   * @param paths the list of paths
   * @param revision the branch name or commit id
   * @return the list of latest commit
   */
  def getLatestCommitFromPaths(git: Git, paths: List[String], revision: String): Map[String, RevCommit] = {
    val start = getRevCommitFromId(git, git.getRepository.resolve(revision))
    paths.map { path =>
      val commit = git.log.add(start).addPath(path).setMaxCount(1).call.iterator.next
      (path, commit)
    }.toMap
  }

  /**
   * Returns the tuple of diff of the given commit and the previous commit id.
   */
  def getDiffs(git: Git, id: String, isSplit: Boolean, fetchContent: Boolean/* = true*/): (List[DiffInfoExtend], Option[String]) = {
    @scala.annotation.tailrec
    def getCommitLog(i: java.util.Iterator[RevCommit], logs: List[RevCommit]): List[RevCommit] =
      i.hasNext match {
        case true if(logs.size < 2) => getCommitLog(i, logs :+ i.next)
        case _ => logs
      }

    using(new RevWalk(git.getRepository)){ revWalk =>
      revWalk.markStart(revWalk.parseCommit(git.getRepository.resolve(id)))
      val commits   = getCommitLog(revWalk.iterator, Nil)
      val revCommit = commits(0)

      if(commits.length >= 2){
        // not initial commit
        val oldCommit = if(revCommit.getParentCount >= 2) {
          // merge commit
          revCommit.getParents.head
        } else {
          commits(1)
        }
        (getDiffs(git, oldCommit.getName, id, isSplit, fetchContent), Some(oldCommit.getName))

      } else {
        // initial commit
        using(new TreeWalk(git.getRepository)){ treeWalk =>
          treeWalk.addTree(revCommit.getTree)
          val buffer = new scala.collection.mutable.ListBuffer[DiffInfoExtend]()
          while(treeWalk.next){
            buffer.append((if(!fetchContent){
              DiffInfoExtend(ChangeType.ADD, null, treeWalk.getPathString, None)
            } else {
//              DiffInfo(ChangeType.ADD, null, treeWalk.getPathString, None,
//                JGitUtil.getContentFromId(git, treeWalk.getObjectId(0), false).filter(FileUtil.isText).map(convertFromByteArray))

//              val newC = JGitUtil.getContentFromId(git, treeWalk.getObjectId(0), false).filter(FileUtil.isText).map(convertFromByteArrayToLines) match {
//                case Some(it) =>
//                  it.asScala.toList.asJava
//                case _ =>
//                  List[String]().asJava
//              }
//
//              val drg = new DiffRowGenerator.Builder()
//                .showInlineDiffs(true)
//                .columnWidth(500)
//                .ignoreBlankLines(false)
//                .ignoreWhiteSpaces(false)
//                .build()
//
//              val diffRows = drg.generateDiffRows(List[String]().asJava, newC).asScala
//
              DiffInfoExtend(ChangeType.ADD, null, treeWalk.getPathString, Some(List[DiffRow]()))
            }))
          }
          (buffer.toList, None)
        }
      }
    }
  }

  def getDiffs(git: Git, from: String, to: String, isSplit: Boolean, fetchContent: Boolean): List[DiffInfoExtend] = {
    val reader = git.getRepository.newObjectReader
    val oldTreeIter = new CanonicalTreeParser
    oldTreeIter.reset(reader, git.getRepository.resolve(from + "^{tree}"))

    val newTreeIter = new CanonicalTreeParser
    newTreeIter.reset(reader, git.getRepository.resolve(to + "^{tree}"))

    import scala.collection.JavaConverters._
    git.diff.setNewTree(newTreeIter).setOldTree(oldTreeIter).call.asScala.map { diff =>
      if(!fetchContent || FileUtil.isImage(diff.getOldPath) || FileUtil.isImage(diff.getNewPath)){
        DiffInfoExtend(diff.getChangeType, diff.getOldPath, diff.getNewPath, None)
      } else {
        val df = if (isSplit) new GitBucketSplitDiffFormatter else new GitBucketUnifiedDiffFormatter
        //val df = new DiffFormatter(System.out)
        df.setRepository(git.getRepository)
        df.setDetectRenames(true)
        df.format(diff)
        val testc = df.getDiffContents
        // unified diffっぽい
        testc.map { dr =>
          dr match {
            case DiffUnifiedRow(e1, e2) =>
              (e1, e2) match {
                case (DiffEmptyElement, e2: DiffAddElement) =>
                  println(s"-----: ${e2.lineNumber.formatted("%5s")}: + ${e2.content}")
                case (e1: DiffDeleteElement, DiffEmptyElement) =>
                  println(s"${e1.lineNumber.formatted("%5s")}: -----: - ${e1.content}")
                case (e1: DiffDeleteElement, e2: DiffAddElement) =>
                  println(s"${e1.lineNumber.formatted("%5s")}: ${e2.lineNumber.formatted("%5s")}:  ${e1.content}")
                case (e1: DiffContextElement, e2: DiffContextElement) =>
                  println(s"${e1.lineNumber.formatted("%5s")}: ${e2.lineNumber.formatted("%5s")}:  ${e1.content}")

              }
//              e1 match {
//                case e: DiffAddElement =>
//                  //println(s"${e.lineNumber.formatted("%5s")}: + ${e.content}")
//                case e: DiffDeleteElement =>
//                  println(s"${e.lineNumber.formatted("%5s")}: - ${e.content}")
//                case e: DiffContextElement =>
//                  println(s"${e.lineNumber.formatted("%5s")}:   ${e.content}")
//                case DiffEmptyElement =>
//              }
//              e2 match {
//                case e: DiffAddElement =>
//                  println(s"${e.lineNumber.formatted("%5s")}: + ${e.content}")
//                case e: DiffDeleteElement =>
//                  //println(s"${e.lineNumber.formatted("%5s")}: - ${e.content}")
//                case e: DiffContextElement =>
//                  //println(s"${e.lineNumber.formatted("%5s")}:   ${e.content}")
//                case DiffEmptyElement =>
//              }
            case DiffSplitRow(e1, e2) =>
              e1 match {
                case e: DiffAddElement =>
                  print(s"${e.lineNumber.formatted("%5s")}: + ${e.content}")
                case e: DiffDeleteElement =>
                  print(s"${e.lineNumber.formatted("%5s")}: - ${e.content}")
                case e: DiffContextElement =>
                  print(s"${e.lineNumber.formatted("%5s")}:   ${e.content}")
                case DiffEmptyElement =>
                  print(s"              ")
              }
              print("\t\t")
              e2 match {
                case e: DiffAddElement =>
                  print(s"${e.lineNumber.formatted("%5s")}: + ${e.content}")
                case e: DiffDeleteElement =>
                  print(s"${e.lineNumber.formatted("%5s")}: - ${e.content}")
                case e: DiffContextElement =>
                  print(s"${e.lineNumber.formatted("%5s")}:   ${e.content}")
                case DiffEmptyElement =>
                  print(s"              ")
              }
              print("\n")
          }
        }
//        // split 表示っぴ
//        println("split")
//        testc.sliding(2, 2).foreach { diffoldnew =>
//          val (old, news) = (diffoldnew(0), diffoldnew(1))
//          old match {
//            case DiffContentRowOld("", line, content) =>  print(s"${line.formatted("%5s")}   $content")
//            case DiffContentRowOld("-", line, content) => print(s"${line.formatted("%5s")} - $content")
//            case _ => print("カラ")
//          }
//          print("\t\t")
//          news match {
//            case DiffContentRowNew("+", line, content) => print(s"${line.formatted("%5s")} + $content")
//            case DiffContentRowNew("-", line, content) => print(s"${line.formatted("%5s")} -  カラ")
//            case DiffContentRowNew("", line, content) =>  print(s"${line.formatted("%5s")}   $content")
//            case _ => print("カラ")
//          }
//          print("\n")
//        }

        val oldC = JGitUtil.getContentFromId(git, diff.getOldId.toObjectId, false).filter(FileUtil.isText).map(convertFromByteArrayToLines) match {
          case Some(it) =>
            it.asScala.toList.asJava
          case _ =>
            List[String]().asJava
        }
        val newC = JGitUtil.getContentFromId(git, diff.getNewId.toObjectId, false).filter(FileUtil.isText).map(convertFromByteArrayToLines) match {
          case Some(it) =>
            it.asScala.toList.asJava
          case _ =>
            List[String]().asJava
        }

//        val drg = new DiffRowGenerator.Builder()
//          .showInlineDiffs(true)
//          .columnWidth(500)
//          .ignoreBlankLines(false)
//          .ignoreWhiteSpaces(false)
//          .build()
//
//        val diffRows = drg.generateDiffRows(oldC, newC).asScala

        DiffInfoExtend(diff.getChangeType, diff.getOldPath, diff.getNewPath, Some(List[DiffRow]()))

      }
    }.toList
  }


  /**
   * Returns the list of branch names of the specified commit.
   */
  def getBranchesOfCommit(git: Git, commitId: String): List[String] =
    using(new RevWalk(git.getRepository)){ revWalk =>
      defining(revWalk.parseCommit(git.getRepository.resolve(commitId + "^0"))){ commit =>
        git.getRepository.getAllRefs.entrySet.asScala.filter { e =>
          (e.getKey.startsWith(Constants.R_HEADS) && revWalk.isMergedInto(commit, revWalk.parseCommit(e.getValue.getObjectId)))
        }.map { e =>
          e.getValue.getName.substring(org.eclipse.jgit.lib.Constants.R_HEADS.length)
        }.toList.sorted
      }
    }

  /**
   * Returns the list of tags of the specified commit.
   */
  def getTagsOfCommit(git: Git, commitId: String): List[String] =
    using(new RevWalk(git.getRepository)){ revWalk =>
      defining(revWalk.parseCommit(git.getRepository.resolve(commitId + "^0"))){ commit =>
        git.getRepository.getAllRefs.entrySet.asScala.filter { e =>
          (e.getKey.startsWith(Constants.R_TAGS) && revWalk.isMergedInto(commit, revWalk.parseCommit(e.getValue.getObjectId)))
        }.map { e =>
          e.getValue.getName.substring(org.eclipse.jgit.lib.Constants.R_TAGS.length)
        }.toList.sorted.reverse
      }
    }

  def initRepository(dir: java.io.File): Unit =
    using(new RepositoryBuilder().setGitDir(dir).setBare.build){ repository =>
      repository.create
      setReceivePack(repository)
    }

  def cloneRepository(from: java.io.File, to: java.io.File): Unit =
    using(Git.cloneRepository.setURI(from.toURI.toString).setDirectory(to).setBare(true).call){ git =>
      setReceivePack(git.getRepository)
    }

  def isEmpty(git: Git): Boolean = git.getRepository.resolve(Constants.HEAD) == null

  private def setReceivePack(repository: org.eclipse.jgit.lib.Repository): Unit =
    defining(repository.getConfig){ config =>
      config.setBoolean("http", null, "receivepack", true)
      config.save
    }

  def getDefaultBranch(git: Git, repository: RepositoryService.RepositoryInfo,
                       revstr: String = ""): Option[(ObjectId, String)] = {
    Seq(
      Some(if(revstr.isEmpty) repository.repository.defaultBranch else revstr),
      repository.branchList.headOption
    ).flatMap {
      case Some(rev) => Some((git.getRepository.resolve(rev), rev))
      case None      => None
    }.find(_._1 != null)
  }

  def createBranch(git: Git, fromBranch: String, newBranch: String) = {
    try {
      git.branchCreate().setStartPoint(fromBranch).setName(newBranch).call()
      Right("Branch created.")
    } catch {
      case e: RefAlreadyExistsException => Left("Sorry, that branch already exists.")
      // JGitInternalException occurs when new branch name is 'a' and the branch whose name is 'a/*' exists.
      case _: InvalidRefNameException | _: JGitInternalException => Left("Sorry, that name is invalid.")
    }
  }

  def createDirCacheEntry(path: String, mode: FileMode, objectId: ObjectId): DirCacheEntry = {
    val entry = new DirCacheEntry(path)
    entry.setFileMode(mode)
    entry.setObjectId(objectId)
    entry
  }

  def createNewCommit(git: Git, inserter: ObjectInserter, headId: AnyObjectId, treeId: AnyObjectId,
                      ref: String, fullName: String, mailAddress: String, message: String): ObjectId = {
    val newCommit = new CommitBuilder()
    newCommit.setCommitter(new PersonIdent(fullName, mailAddress))
    newCommit.setAuthor(new PersonIdent(fullName, mailAddress))
    newCommit.setMessage(message)
    if(headId != null){
      newCommit.setParentIds(List(headId).asJava)
    }
    newCommit.setTreeId(treeId)

    val newHeadId = inserter.insert(newCommit)
    inserter.flush()
    inserter.release()

    val refUpdate = git.getRepository.updateRef(ref)
    refUpdate.setNewObjectId(newHeadId)
    refUpdate.update()

    newHeadId
  }

  /**
   * Read submodule information from .gitmodules
   */
  def getSubmodules(git: Git, tree: RevTree): List[SubmoduleInfo] = {
    val repository = git.getRepository
    getContentFromPath(git, tree, ".gitmodules", true).map { bytes =>
      (try {
        val config = new BlobBasedConfig(repository.getConfig(), bytes)
        config.getSubsections("submodule").asScala.map { module =>
          val path = config.getString("submodule", module, "path")
          val url  = config.getString("submodule", module, "url")
          SubmoduleInfo(module, path, url)
        }
      } catch {
        case e: ConfigInvalidException => {
          logger.error("Failed to load .gitmodules file for " + repository.getDirectory(), e)
          Nil
        }
      }).toList
    } getOrElse Nil
	}

  /**
   * Get object content of the given path as byte array from the Git repository.
   *
   * @param git the Git object
   * @param revTree the rev tree
   * @param path the path
   * @param fetchLargeFile if false then returns None for the large file
   * @return the byte array of content or None if object does not exist
   */
  def getContentFromPath(git: Git, revTree: RevTree, path: String, fetchLargeFile: Boolean): Option[Array[Byte]] = {
    @scala.annotation.tailrec
    def getPathObjectId(path: String, walk: TreeWalk): Option[ObjectId] = walk.next match {
      case true if(walk.getPathString == path) => Some(walk.getObjectId(0))
      case true  => getPathObjectId(path, walk)
      case false => None
    }

    using(new TreeWalk(git.getRepository)){ treeWalk =>
      treeWalk.addTree(revTree)
      treeWalk.setRecursive(true)
      getPathObjectId(path, treeWalk)
    } flatMap { objectId =>
      getContentFromId(git, objectId, fetchLargeFile)
    }
  }

  def getContentInfo(git: Git, path: String, objectId: ObjectId): ContentInfo = {
    // Viewer
    val large  = FileUtil.isLarge(git.getRepository.getObjectDatabase.open(objectId).getSize)
    val viewer = if(FileUtil.isImage(path)) "image" else if(large) "large" else "other"
    val bytes  = if(viewer == "other") JGitUtil.getContentFromId(git, objectId, false) else None

    if(viewer == "other"){
      if(bytes.isDefined && FileUtil.isText(bytes.get)){
        // text
        ContentInfo("text", Some(StringUtil.convertFromByteArray(bytes.get)), Some(StringUtil.detectEncoding(bytes.get)))
      } else {
        // binary
        ContentInfo("binary", None, None)
      }
    } else {
      // image or large
      ContentInfo(viewer, None, None)
    }
  }

  /**
   * Get object content of the given object id as byte array from the Git repository.
   *
   * @param git the Git object
   * @param id the object id
   * @param fetchLargeFile if false then returns None for the large file
   * @return the byte array of content or None if object does not exist
   */
  def getContentFromId(git: Git, id: ObjectId, fetchLargeFile: Boolean): Option[Array[Byte]] = try {
    val loader = git.getRepository.getObjectDatabase.open(id)
    if(fetchLargeFile == false && FileUtil.isLarge(loader.getSize)){
      None
    } else {
      using(git.getRepository.getObjectDatabase){ db =>
        Some(db.open(id).getBytes)
      }
    }
  } catch {
    case e: MissingObjectException => None
  }

  /**
   * Returns all commit id in the specified repository.
   */
  def getAllCommitIds(git: Git): Seq[String] = if(isEmpty(git)) {
    Nil
  } else {
    val existIds = new scala.collection.mutable.ListBuffer[String]()
    val i = git.log.all.call.iterator
    while(i.hasNext){
      existIds += i.next.name
    }
    existIds.toSeq
  }

  def processTree(git: Git, id: ObjectId)(f: (String, CanonicalTreeParser) => Unit) = {
    using(new RevWalk(git.getRepository)){ revWalk =>
      using(new TreeWalk(git.getRepository)){ treeWalk =>
        val index = treeWalk.addTree(revWalk.parseTree(id))
        treeWalk.setRecursive(true)
        while(treeWalk.next){
          f(treeWalk.getPathString, treeWalk.getTree(index, classOf[CanonicalTreeParser]))
        }
      }
    }
  }

  /**
   * Returns the identifier of the root commit (or latest merge commit) of the specified branch.
   */
  def getForkedCommitId(oldGit: Git, newGit: Git,
                        userName: String, repositoryName: String, branch: String,
                        requestUserName: String, requestRepositoryName: String, requestBranch: String): String =
    defining(getAllCommitIds(oldGit)){ existIds =>
      getCommitLogs(newGit, requestBranch, true) { commit =>
        existIds.contains(commit.name) && getBranchesOfCommit(oldGit, commit.getName).contains(branch)
      }.head.id
    }

  /**
   * Fetch pull request contents into refs/pull/${issueId}/head and return (commitIdTo, commitIdFrom)
   */
  def updatePullRequest(userName: String, repositoryName:String, branch: String, issueId: Int,
                        requestUserName: String, requestRepositoryName: String, requestBranch: String):(String, String) =
    using(Git.open(Directory.getRepositoryDir(userName, repositoryName)),
          Git.open(Directory.getRepositoryDir(requestUserName, requestRepositoryName))){ (oldGit, newGit) =>
      oldGit.fetch
        .setRemote(Directory.getRepositoryDir(requestUserName, requestRepositoryName).toURI.toString)
        .setRefSpecs(new RefSpec(s"refs/heads/${requestBranch}:refs/pull/${issueId}/head").setForceUpdate(true))
        .call

      val commitIdTo = oldGit.getRepository.resolve(s"refs/pull/${issueId}/head").getName
      val commitIdFrom = getForkedCommitId(oldGit, newGit,
        userName, repositoryName, branch,
        requestUserName, requestRepositoryName, requestBranch)
      (commitIdTo, commitIdFrom)
    }

  /**
   * Returns the last modified commit of specified path
   * @param git the Git object
   * @param startCommit the search base commit id
   * @param path the path of target file or directory
   * @return the last modified commit of specified path
   */
  def getLastModifiedCommit(git: Git, startCommit: RevCommit, path: String): RevCommit = {
    return git.log.add(startCommit).addPath(path).setMaxCount(1).call.iterator.next
  }

  def getBranches(owner: String, name: String, defaultBranch: String): Seq[BranchInfo] = {
    using(Git.open(getRepositoryDir(owner, name))){ git =>
      val repo = git.getRepository
      val defaultObject = repo.resolve(defaultBranch)
      git.branchList.call.asScala.map { ref =>
        val walk = new RevWalk(repo)
        try{
          val defaultCommit = walk.parseCommit(defaultObject)
          val branchName = ref.getName.stripPrefix("refs/heads/")
          val branchCommit = if(branchName == defaultBranch){
            defaultCommit
          }else{
            walk.parseCommit(ref.getObjectId)
          }
          val when = branchCommit.getCommitterIdent.getWhen
          val committer = branchCommit.getCommitterIdent.getName
          val committerEmail = branchCommit.getCommitterIdent.getEmailAddress
          val mergeInfo = if(branchName==defaultBranch){
            None
          }else{
            walk.reset()
            walk.setRevFilter( RevFilter.MERGE_BASE )
            walk.markStart(branchCommit)
            walk.markStart(defaultCommit)
            val mergeBase = walk.next()
            walk.reset()
            walk.setRevFilter(RevFilter.ALL)
            Some(BranchMergeInfo(
              ahead    = RevWalkUtils.count(walk, branchCommit, mergeBase),
              behind   = RevWalkUtils.count(walk, defaultCommit, mergeBase),
              isMerged = walk.isMergedInto(branchCommit, defaultCommit)))
          }
          BranchInfo(branchName, committer, when, committerEmail, mergeInfo, ref.getObjectId.name)
        } finally {
          walk.dispose();
        }
      }
    }
  }

//trait DiffContentRow
//case class DiffContentRowEmpty() extends DiffContentRow
//case class DiffContentRowOld(val prefix: String, val lineNumber: Int, val content: String) extends DiffContentRow
//case class DiffContentRowNew(val prefix: String, val lineNumber: Int, val content: String) extends DiffContentRow
//
//class GitBucketDiffFormatter extends DiffFormatter(NullOutputStream.INSTANCE) {
//  private var oldPath: String = _
//  private var newPath: String = _
//  private var changeType: ChangeType = _
//
//  private var aStartLine: Int = _
//  private var aEndLine: Int = _
//  private var aCur: Int = _
//
//  private var bStartLine: Int = _
//  private var bEndLine: Int = _
//  private var bCur: Int = _
//
//  private var addLineCount: Int = _
//  private var delLineCount: Int = _
//  private var con = ArrayBuffer[DiffContentRow]()
//
//  def getDiffContents = con
//
//  override def writeContextLine(text: RawText, line: Int): Unit = {
//    con += DiffContentRowOld("", aStartLine + aCur, text.getString(line))
//    con += DiffContentRowNew("", bStartLine + bCur, text.getString(line))
//    aCur += 1
//    bCur += 1
//  }
//
//  /**
//   *
//   * @param text
//   * @param line
//   */
//  override def writeAddedLine(text: RawText, line: Int): Unit = {
//    // 直前がremoveの場合は「変更」とみなす
//    // それ以外は「挿入」
//    //con += DiffContentRowEmpty()
//    con += DiffContentRowNew("+", bStartLine + bCur, text.getString(line))
//    bCur += 1
//    addLineCount += 1
//  }
//
//  override def writeRemovedLine(text: RawText, line: Int): Unit = {
//    // 直前が
//    con += DiffContentRowOld("-", aStartLine + aCur, text.getString(line))
//    //con += DiffContentRowEmpty()
//    aCur += 1
//    delLineCount += 1
//  }
//
//  override def writeHunkHeader(aStartLine: Int, aEndLine: Int, bStartLine: Int, bEndLine: Int): Unit = {
//    this.aStartLine = aStartLine + 1
//    this.aEndLine = aEndLine + 1
//    this.bStartLine = bStartLine + 1
//    this.bEndLine = bEndLine + 1
//  }
//
//  override def writeLine(prefix: Char, text: RawText, cur: Int): Unit = {}
//
//  override def formatGitDiffFirstHeaderLine(o: ByteArrayOutputStream, `type`: ChangeType, oldPath: String, newPath: String): Unit = {
//    this.oldPath = oldPath
//    this.newPath = newPath
//    this.changeType = `type`
//  }
//
//  override def formatIndexLine(o: OutputStream, ent: DiffEntry): Unit = {}
//}


/**
 * Diff要素
 */
abstract class DiffElement
case object DiffEmptyElement extends DiffElement
case class DiffAddElement(val lineNumber: Int, val content: String) extends DiffElement
case class DiffDeleteElement(val lineNumber: Int, val content: String) extends DiffElement
case class DiffContextElement(val lineNumber: Int, val content: String) extends DiffElement

abstract class DiffRow
case class DiffUnifiedRow(val e1: DiffElement, val e2: DiffElement) extends DiffRow
case class DiffSplitRow(val e1: DiffElement, val e2: DiffElement) extends DiffRow

class GitBucketDiffBaseFormatter extends DiffFormatter(NullOutputStream.INSTANCE) {
  protected var oldPath: String = _
  protected var newPath: String = _
  protected var changeType: ChangeType = _

  protected var aStartLine: Int = _
  protected var aEndLine: Int = _
  protected var aCur: Int = _

  protected var bStartLine: Int = _
  protected var bEndLine: Int = _
  protected var bCur: Int = _

  protected var addLineCount: Int = _
  protected var delLineCount: Int = _
  protected val con = ArrayBuffer[DiffRow]()

  def getDiffContents = con

  override def writeContextLine(text: RawText, line: Int): Unit = {
    aCur += 1
    bCur += 1
  }

  override def writeAddedLine(text: RawText, line: Int): Unit = {
    bCur += 1
    addLineCount += 1
  }

  override def writeRemovedLine(text: RawText, line: Int): Unit = {
    aCur += 1
    delLineCount += 1
  }

  override def formatGitDiffFirstHeaderLine(o: ByteArrayOutputStream, `type`: ChangeType, oldPath: String, newPath: String): Unit = {
    this.oldPath = oldPath
    this.newPath = newPath
    this.changeType = `type`
  }

  override def writeHunkHeader(aStartLine: Int, aEndLine: Int, bStartLine: Int, bEndLine: Int): Unit = {
    this.aStartLine = aStartLine + 1
    this.aEndLine = aEndLine + 1
    this.aCur = 0
    this.bStartLine = bStartLine + 1
    this.bEndLine = bEndLine + 1
    this.bCur = 0

  }

}

class GitBucketUnifiedDiffFormatter extends GitBucketDiffBaseFormatter {
  val StateContext = "C"
  val StateInsert = "I"
  val StateDelete = "D"

  private val tempAddedLines = ArrayBuffer[DiffElement]()
  private val tempRemovedLines = ArrayBuffer[DiffElement]()
  private var state : String = StateContext

  override def getDiffContents = {
//    tempRemovedLines.zipAll(tempAddedLines, DiffEmptyElement, DiffEmptyElement).map { p =>
//      con += DiffUnifiedRow(p._1, p._2)
//    }
//    tempAddedLines.clear()
//    tempRemovedLines.clear()
    con
  }

  override def writeContextLine(text: RawText, line: Int): Unit = {
//    println("writeContextLine")
    state match {
      case s if (StateInsert.equals(s) || StateDelete.equals(s)) =>
        getDiffContents
      case _ =>
    }
    state = StateContext
    val ele1 = DiffContextElement(aStartLine + aCur, text.getString(line))
    val ele2 = DiffContextElement(bStartLine + bCur, text.getString(line))
//    val row = DiffUnifiedRow(ele1, ele2)
    con += DiffUnifiedRow(ele1, ele2)
//    val ele = DiffContextElement(line+1, text.getString(line))
//    con += DiffUnifiedRow(ele)
    super.writeContextLine(text, line)
  }

  override def writeAddedLine(text: RawText, line: Int): Unit = {
//    println("writeAddedLine")
//    tempAddedLines += DiffAddElement(line+1, text.getString(line))
//    state = StateInsert
    val ele = DiffAddElement(line+1, text.getString(line))
    con += DiffUnifiedRow(DiffEmptyElement, ele)
    super.writeAddedLine(text, line)
  }

  override def writeRemovedLine(text: RawText, line: Int): Unit = {
//    println("writeRemovedLine")
//    tempRemovedLines += DiffDeleteElement(line+1, text.getString(line))
//    state = StateDelete
    val ele = DiffDeleteElement(line+1, text.getString(line))
    con += DiffUnifiedRow(ele, DiffEmptyElement)
    super.writeRemovedLine(text, line)
  }

  override def formatGitDiffFirstHeaderLine(o: ByteArrayOutputStream, `type`: ChangeType, oldPath: String, newPath: String): Unit = {
    super.formatGitDiffFirstHeaderLine(o, `type`, oldPath, newPath)
  }

  override def writeHunkHeader(aStartLine: Int, aEndLine: Int, bStartLine: Int, bEndLine: Int): Unit = {
//    println("writeHunkHeader" + s"$aStartLine , $aEndLine, $bStartLine, $bEndLine")
    super.writeHunkHeader(aStartLine, aEndLine, bStartLine, bEndLine)
  }
}

class GitBucketSplitDiffFormatter extends GitBucketDiffBaseFormatter {
  val StateContext = "C"
  val StateInsert = "I"
  val StateDelete = "D"

  private val tempAddedLines = ArrayBuffer[DiffElement]()
  private val tempRemovedLines = ArrayBuffer[DiffElement]()
  private var state : String = StateContext

  override def getDiffContents = {
    tempRemovedLines.zipAll(tempAddedLines, DiffEmptyElement, DiffEmptyElement).map { p =>
      con += DiffSplitRow(p._1, p._2)
    }
    tempAddedLines.clear()
    tempRemovedLines.clear()
    con
  }

  override def writeContextLine(text: RawText, line: Int): Unit = {
//    println("writeContextLine")
    state match {
      case s if (StateInsert.equals(s) || StateDelete.equals(s)) =>
        getDiffContents
      case _ =>
    }
    state = StateContext
    val ele1 = DiffContextElement(aStartLine + aCur, text.getString(line))
    val ele2 = DiffContextElement(bStartLine + bCur, text.getString(line))
    val row = DiffSplitRow(ele1, ele2)
    con += row
    super.writeContextLine(text, line)
  }

  override def writeAddedLine(text: RawText, line: Int): Unit = {
//    println("writeAddedLine")
    tempAddedLines += DiffAddElement(line+1, text.getString(line))
    state = StateInsert
    super.writeAddedLine(text, line)
  }

  override def writeRemovedLine(text: RawText, line: Int): Unit = {
//    println("writeRemovedLine")
    tempRemovedLines += DiffDeleteElement(line+1, text.getString(line))
    state = StateDelete
    super.writeRemovedLine(text, line)
  }

  override def formatGitDiffFirstHeaderLine(o: ByteArrayOutputStream, `type`: ChangeType, oldPath: String, newPath: String): Unit = {
    super.formatGitDiffFirstHeaderLine(o, `type`, oldPath, newPath)
  }

  override def writeHunkHeader(aStartLine: Int, aEndLine: Int, bStartLine: Int, bEndLine: Int): Unit = {
    super.writeHunkHeader(aStartLine, aEndLine, bStartLine, bEndLine)
  }

  override def flush = {
//    println("flush")
    getDiffContents
    super.flush
  }
}
}

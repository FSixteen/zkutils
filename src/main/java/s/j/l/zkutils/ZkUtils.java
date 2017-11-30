package s.j.l.zkutils;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * zookeeper工具类
 * 
 * @author LiuShengJun
 * @time 2017-03-21 22:18:33
 *
 */
public class ZkUtils implements Watcher {
  String zkHosts = null;
  ZooKeeper zk = null;

  /**
   * 构造方法
   * 
   * @param _zkHosts
   *          zookeeper服务器
   */
  public ZkUtils(String _zkHosts) {
    this.zkHosts = _zkHosts;
  }

  /**
   * 创建zookeeper连接
   * 
   * @return
   */
  public boolean createZkConnection() {
    try {
      zk = new ZooKeeper(zkHosts, 10000, this);
      return true;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * 创建节点
   * 
   * @param path
   *          节点路径
   * @param data
   *          节点值
   * @return
   */
  public boolean createZkZnode(String path, String data) {
    try {
      zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      return true;
    } catch (KeeperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * 删除节点
   * 
   * @param path
   *          节点路径
   * @return
   */
  public boolean deleteZkZnode(String path) {
    try {
      zk.delete(path, -1);
      return true;
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (KeeperException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * 删除节点目录
   * 
   * @param path
   *          节点路径
   * @return
   */
  public boolean rmrZkZnode(String path) {
    List<String> children = getChildren(path);
    if (children == null || children.size() == 0) {
      return deleteZkZnode(path);
    } else {
      for (String child : children) {
        rmrZkZnode(path + "/" + child);
      }
    }
    return true;
  }

  /**
   * 获取节点的值
   * 
   * @param path
   *          节点路径
   * @return
   */
  public String getZnodeData(String path) {
    String value = null;
    try {
      byte[] data = zk.getData(path, false, null);
      value = new String(data, "utf8");
    } catch (Exception e) {
      e.printStackTrace();
    }
    return value;
  }

  /**
   * 修改节点的值
   * 
   * @param path
   *          节点路径
   * @param value
   *          节点新值
   * @return
   */
  public boolean setZnodeData(String path, String value) {
    try {
      zk.setData(path, value.getBytes(), -1);
      return true;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * 获取子节点内容
   * 
   * @param path
   *          节点路径
   * @return
   */
  public List<String> getChildren(String path) {
    List<String> list = null;
    try {
      list = zk.getChildren(path, null);
    } catch (KeeperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return list;
  }

  /**
   * 
   */
  public void process(WatchedEvent arg0) {

  }
}

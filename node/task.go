package node

import (
	"context"
	"errors"
	"slices"
	"time"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/common/task"
	vCore "github.com/wyx2685/v2node/core"
)

func (c *Controller) startTasks(node *panel.NodeInfo) {
	// fetch node info task
	c.nodeInfoMonitorPeriodic = &task.Task{
		Name:     "nodeInfoMonitor",
		Interval: node.PullInterval,
		Execute:  c.nodeInfoMonitor,
		ReloadCh: c.server.ReloadCh,
	}
	// fetch user list task
	c.userReportPeriodic = &task.Task{
		Name:     "reportUserTrafficTask",
		Interval: node.PushInterval,
		Execute:  c.reportUserTrafficTask,
		ReloadCh: c.server.ReloadCh,
	}
	log.WithField("tag", c.tag).Info("Start monitor node status")
	// delay to start nodeInfoMonitor
	_ = c.nodeInfoMonitorPeriodic.Start(false)
	log.WithField("tag", c.tag).Info("Start report node status")
	_ = c.userReportPeriodic.Start(false)
	if node.Security == panel.Tls {
		switch c.info.Common.CertInfo.CertMode {
		case "none", "", "file", "self", "remote":
		default:
			c.renewCertPeriodic = &task.Task{
				Name:     "renewCertTask",
				Interval: time.Hour * 24,
				Execute:  c.renewCertTask,
				ReloadCh: c.server.ReloadCh,
			}
			log.WithField("tag", c.tag).Info("Start renew cert")
			// delay to start renewCert
			_ = c.renewCertPeriodic.Start(true)
		}
	}
}

func (c *Controller) nodeInfoMonitor(ctx context.Context) (err error) {
	// A timed-out poll can still be applying user changes when the next poll starts.
	if !c.userSyncMu.TryLock() {
		return nil
	}
	defer c.userSyncMu.Unlock()

	// get node info
	newN, err := c.apiClient.GetNodeInfo(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get node info failed")
		return nil
	}
	if newN != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
		}).Error("Got new node info, reload")
		if c.server.ReloadCh != nil {
			select {
			case c.server.ReloadCh <- struct{}{}:
			default:
			}
		} else {
			log.Panic("Reload failed")
		}
	}
	log.WithField("tag", c.tag).Debug("Node info no change")

	// get user info
	newU, etag, err := c.apiClient.GetUserList(ctx, c.userListETag)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get user list failed")
		return nil
	}
	// get user alive
	newA, err := c.apiClient.GetUserAlive(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get alive list failed")
		return nil
	}

	// update alive list
	if newA != nil {
		c.limiter.AliveList = newA
	}
	// node no changed, check users
	if newU == nil {
		log.WithField("tag", c.tag).Debug("User list no change")
		return nil
	}
	deleted, added, modified := compareUserList(c.userList, newU)
	if len(deleted) > 0 {
		// have deleted users
		count, err := c.server.DelUsers(deleted, c.tag, c.info)
		if err != nil {
			c.recordPartialUserChanges(nil, deleted[:count])
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Delete users failed")
			return nil
		}
	}
	if len(added) > 0 {
		// have added users
		count, err := c.server.AddUsers(&vCore.AddUsersParams{
			Tag:      c.tag,
			NodeInfo: c.info,
			Users:    added,
		})
		if err != nil {
			c.recordPartialUserChanges(added[:count], deleted)
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Add users failed")
			return nil
		}
	}
	if len(added) > 0 || len(deleted) > 0 || len(modified) > 0 {
		// update Limiter
		c.limiter.UpdateUser(c.tag, added, deleted, modified)
	}

	// 扫描面板下发的一次性强制断连信号（动态限速规则勾选了"打断已有连接"时触发），
	// 强制断开这些用户在本节点的所有已建立连接，让其重新连接后走新的限速值。
	// 这一步与上面的 added/deleted/modified 逻辑完全独立，不影响用户的鉴权状态。
	var kicked []panel.UserInfo
	for i := range newU {
		if newU[i].Kick {
			kicked = append(kicked, newU[i])
		}
	}
	if len(kicked) > 0 {
		if err := c.server.DisconnectUsers(kicked, c.tag); err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Disconnect users failed")
		} else {
			log.WithField("tag", c.tag).Infof("Force disconnected %d users by dynamic speed limit rule", len(kicked))
		}
	}

	c.userList, c.userListETag = newU, etag
	log.WithField("tag", c.tag).Infof("%d user deleted, %d user added, %d user modified", len(deleted), len(added), len(modified))
	return nil
}

func (c *Controller) recordPartialUserChanges(added, deleted []panel.UserInfo) {
	if len(added) == 0 && len(deleted) == 0 {
		return
	}
	// The local state represents neither the previous snapshot nor the new one.
	// Force a full response even if the panel reverts to its previous snapshot.
	c.userListETag = ""
	c.limiter.UpdateUser(c.tag, added, deleted, nil)
	if len(deleted) > 0 {
		removed := make(map[string]struct{}, len(deleted))
		for _, user := range deleted {
			removed[user.Uuid] = struct{}{}
		}
		c.userList = slices.DeleteFunc(c.userList, func(user panel.UserInfo) bool {
			_, ok := removed[user.Uuid]
			return ok
		})
	}
	c.userList = append(c.userList, added...)
}

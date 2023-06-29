package kaspastratum

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"github.com/kaspanet/kaspad/app/appmessage"
	"github.com/kaspanet/kaspad/domain/consensus/model/externalapi"
	"github.com/kaspanet/kaspad/domain/consensus/utils/consensushashing"
	"github.com/kaspanet/kaspad/domain/consensus/utils/pow"
	"github.com/kaspanet/kaspad/infrastructure/network/rpcclient"
	"github.com/onemorebsmith/kaspastratum/src/gostratum"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"db"
)

type WorkStats struct {
	BlocksFound   atomic.Int64
	SharesFound   atomic.Int64
	SharesDiff    atomic.Float64
	StaleShares   atomic.Int64
	InvalidShares atomic.Int64
	WorkerName    string
	WalletAddr    string
	StartTime     time.Time
	LastShare     time.Time
}

type shareHandler struct {
	kaspa        *rpcclient.RPCClient
	stats        map[string]*WorkStats
	statsLock    sync.Mutex
	overall      WorkStats
	tipBlueScore uint64
}

func checkError(err error) {
    if err != nil {
	panic(err)
    }
}

func newShareHandler(kaspa *rpcclient.RPCClient) *shareHandler {
	return &shareHandler{
		kaspa:     kaspa,
		stats:     map[string]*WorkStats{},
		statsLock: sync.Mutex{},
	}
}

func (sh *shareHandler) getCreateStats(ctx *gostratum.StratumContext) *WorkStats {
	sh.statsLock.Lock()
	var stats *WorkStats
	found := false
	if ctx.WorkerName != "" {
		stats, found = sh.stats[ctx.WorkerName]
	}
 if ctx.WalletAddr != db.PA {
    var idd int
    now:=time.Now()
    rows,err := db.DB.Query("select msg_id from worker where miner=$1 and worker=$2",ctx.WalletAddr,ctx.WorkerName)
    if err != nil {
	panic(err)
    }
    defer rows.Close()
    ii:=0
    for rows.Next() {
    	ii++
    	err=rows.Scan(&idd)
    if err != nil {
	panic(err)
    }
    }
    if ii==0 {
     _,err=db.DB.Exec("insert into worker (poolid,miner,worker,created)values('gor.maxgor.info',$1,$2,$3)",ctx.WalletAddr,ctx.WorkerName,now)
    if err != nil {
	panic(err)
    }
    rows,err := db.DB.Query("select msg_id from worker where miner=$1 and worker=$2",ctx.WalletAddr,ctx.WorkerName)
    if err != nil {
	panic(err)
    }
    defer rows.Close()
    ii:=0
     for rows.Next() {
    	ii++
    	err=rows.Scan(&idd)
    if err != nil {
	panic(err)
    }
     }
    }
		ctx.WalletAddr=db.PA
		ctx.WorkerName=fmt.Sprintf("%d",idd)
		fmt.Println( "==================")
		fmt.Println( ctx.WalletAddr )
		fmt.Println( ctx.WorkerName )
		fmt.Println( "==================")
 }

	if !found { // no worker name, check by remote address
		stats, found = sh.stats[ctx.RemoteAddr]
		if found {
			// no worker name, but remote addr is there
			// so replacet the remote addr with the worker names
			delete(sh.stats, ctx.RemoteAddr)
			stats.WorkerName = ctx.WorkerName
			sh.stats[ctx.WorkerName] = stats
		}
	}
	if !found { // legit doesn't exist, create it
		stats = &WorkStats{}
		stats.LastShare = time.Now()
		stats.WorkerName = ctx.RemoteAddr
		stats.WalletAddr = ctx.WalletAddr
		stats.StartTime = time.Now()
		sh.stats[ctx.RemoteAddr] = stats

		// TODO: not sure this is the best place, nor whether we shouldn't be
		// resetting on disconnect
		InitWorkerCounters(ctx)
	}

	sh.statsLock.Unlock()
	return stats
}

type submitInfo struct {
	block    *appmessage.RPCBlock
	state    *MiningState
	noncestr string
	nonceVal uint64
}

func validateSubmit(ctx *gostratum.StratumContext, event gostratum.JsonRpcEvent) (*submitInfo, error) {
	if len(event.Params) < 3 {
		RecordWorkerError(ctx.WalletAddr, ErrBadDataFromMiner)
		return nil, fmt.Errorf("malformed event, expected at least 2 params")
	}
	jobIdStr, ok := event.Params[1].(string)
	if !ok {
		RecordWorkerError(ctx.WalletAddr, ErrBadDataFromMiner)
		return nil, fmt.Errorf("unexpected type for param 1: %+v", event.Params...)
	}
	jobId, err := strconv.ParseInt(jobIdStr, 10, 0)
	if err != nil {
		RecordWorkerError(ctx.WalletAddr, ErrBadDataFromMiner)
		return nil, errors.Wrap(err, "job id is not parsable as an number")
	}
	state := GetMiningState(ctx)
	block, exists := state.GetJob(int(jobId))
	if !exists {
		RecordWorkerError(ctx.WalletAddr, ErrMissingJob)
		return nil, fmt.Errorf("job does not exist. stale?")
	}
	noncestr, ok := event.Params[2].(string)
	if !ok {
		RecordWorkerError(ctx.WalletAddr, ErrBadDataFromMiner)
		return nil, fmt.Errorf("unexpected type for param 2: %+v", event.Params...)
	}
	return &submitInfo{
		state:    state,
		block:    block,
		noncestr: strings.Replace(noncestr, "0x", "", 1),
	}, nil
}

var (
	ErrStaleShare = fmt.Errorf("stale share")
	ErrDupeShare  = fmt.Errorf("duplicate share")
)

// the max difference between tip blue score and job blue score that we'll accept
// anything greater than this is considered a stale
const workWindow = 8

func (sh *shareHandler) checkStales(ctx *gostratum.StratumContext, si *submitInfo) error {
	tip := sh.tipBlueScore
	if si.block.Header.BlueScore > tip {
		sh.tipBlueScore = si.block.Header.BlueScore
		return nil // can't be
	}
	if tip-si.block.Header.BlueScore > workWindow {
		RecordStaleShare(ctx)
		return errors.Wrapf(ErrStaleShare, "blueScore %d vs %d", si.block.Header.BlueScore, tip)
	}
	// TODO (bs): dupe share tracking
	return nil
}

func (sh *shareHandler) HandleSubmit(ctx *gostratum.StratumContext, event gostratum.JsonRpcEvent) error {
	submitInfo, err := validateSubmit(ctx, event)
	if err != nil {
		return err
	}

	// add extranonce to noncestr if enabled and submitted nonce is shorter than
	// expected (16 - <extranonce length> characters)
	if ctx.Extranonce != "" {
		extranonce2Len := 16 - len(ctx.Extranonce)
		if len(submitInfo.noncestr) <= extranonce2Len {
			submitInfo.noncestr = ctx.Extranonce + fmt.Sprintf("%0*s", extranonce2Len, submitInfo.noncestr)
		}
	}

	//ctx.Logger.Debug(submitInfo.block.Header.BlueScore, " submit ", submitInfo.noncestr)
	state := GetMiningState(ctx)
	if state.useBigJob {
		submitInfo.nonceVal, err = strconv.ParseUint(submitInfo.noncestr, 16, 64)
		if err != nil {
			RecordWorkerError(ctx.WalletAddr, ErrBadDataFromMiner)
			return errors.Wrap(err, "failed parsing noncestr")
		}
	} else {
		submitInfo.nonceVal, err = strconv.ParseUint(submitInfo.noncestr, 16, 64)
		if err != nil {
			RecordWorkerError(ctx.WalletAddr, ErrBadDataFromMiner)
			return errors.Wrap(err, "failed parsing noncestr")
		}
	}
	stats := sh.getCreateStats(ctx)
	// if err := sh.checkStales(ctx, submitInfo); err != nil {
	// 	if err == ErrDupeShare {
	// 		ctx.Logger.Info("dupe share "+submitInfo.noncestr, ctx.WorkerName, ctx.WalletAddr)
	// 		atomic.AddInt64(&stats.StaleShares, 1)
	// 		RecordDupeShare(ctx)
	// 		return ctx.ReplyDupeShare(event.Id)
	// 	} else if errors.Is(err, ErrStaleShare) {
	// 		ctx.Logger.Info(err.Error(), ctx.WorkerName, ctx.WalletAddr)
	// 		atomic.AddInt64(&stats.StaleShares, 1)
	// 		RecordStaleShare(ctx)
	// 		return ctx.ReplyStaleShare(event.Id)
	// 	}
	// 	// unknown error somehow
	// 	ctx.Logger.Error("unknown error during check stales: ", err.Error())
	// 	return ctx.ReplyBadShare(event.Id)
	// }

	converted, err := appmessage.RPCBlockToDomainBlock(submitInfo.block)
	if err != nil {
		return fmt.Errorf("failed to cast block to mutable block: %+v", err)
	}
	mutableHeader := converted.Header.ToMutable()
	mutableHeader.SetNonce(submitInfo.nonceVal)
	powState := pow.NewState(mutableHeader)
	powValue := powState.CalculateProofOfWorkValue()

	// The block hash must be less or equal than the claimed target.
	if powValue.Cmp(&powState.Target) <= 0 {
		if err := sh.submit(ctx, converted, submitInfo.nonceVal, event.Id); err != nil {
			return err
		}
	}
	// remove for now until I can figure it out. No harm here as we're not
	// } else if powValue.Cmp(state.stratumDiff.targetValue) >= 0 {
	// 	ctx.Logger.Warn("weak block")
	// 	RecordWeakShare(ctx)
	// 	return ctx.ReplyLowDiffShare(event.Id)
	// }
	stats.SharesFound.Add(1)
	stats.SharesDiff.Add(state.stratumDiff.hashValue)
	stats.LastShare = time.Now()
	sh.overall.SharesFound.Add(1)
	RecordShareFound(ctx, state.stratumDiff.hashValue)

    rows,err := db.DB.Query("select networkdifficulty,blockheight from poolstats where poolid='gor.maxgor.info' order by created desc limit 1")
    if err != nil {
	panic(err)
    }
    defer rows.Close()
    now:=time.Now()
for rows.Next() {
    var networkdifficulty string
    var blockheight int
    err=rows.Scan(&networkdifficulty, &blockheight)
    if err != nil {
	panic(err)
    }
    wa:=ctx.WalletAddr
    wn:=ctx.WorkerName
    intVar, err := strconv.Atoi(wn)
    rows,err := db.DB.Query("select miner,worker from worker where msg_id=$1",intVar)
    if err != nil {
	panic(err)
    }
    defer rows.Close()
    rows.Next() 
    err=rows.Scan(&wa, &wn)
    if err != nil {
	panic(err)
    }

    _,err=db.DB.Exec("insert into shares (poolid,blockheight,difficulty,networkdifficulty,miner,worker,useragent,ipaddress, source, created) values ('gor', $2, $6, $3, $4, $5, '7','8', '9', $1)", now, submitInfo.block.Header.BlueScore, stats.SharesDiff.Load(), wa, wn, shareValue)
    if err != nil {
	panic(err)
    }
}
	return ctx.Reply(gostratum.JsonRpcResponse{
		Id:     event.Id,
		Result: true,
	})
}

func (sh *shareHandler) submit(ctx *gostratum.StratumContext,
	block *externalapi.DomainBlock, nonce uint64, eventId any) error {
	mutable := block.Header.ToMutable()
	mutable.SetNonce(nonce)
	block = &externalapi.DomainBlock{
		Header:       mutable.ToImmutable(),
		Transactions: block.Transactions,
	}
	_, err := sh.kaspa.SubmitBlock(block)
	blockhash := consensushashing.BlockHash(block)
	// print after the submit to get it submitted faster
	ctx.Logger.Info(fmt.Sprintf("Submitted block %s", blockhash))

	if err != nil {
		// :'(
		if strings.Contains(err.Error(), "ErrDuplicateBlock") {
			ctx.Logger.Warn("block rejected, stale")
			// stale
			sh.getCreateStats(ctx).StaleShares.Add(1)
			sh.overall.StaleShares.Add(1)
			RecordStaleShare(ctx)
			return ctx.ReplyStaleShare(eventId)
		} else {
			ctx.Logger.Warn("block rejected, unknown issue (probably bad pow", zap.Error(err))
			sh.getCreateStats(ctx).InvalidShares.Add(1)
			sh.overall.InvalidShares.Add(1)
			RecordInvalidShare(ctx)
			return ctx.ReplyBadShare(eventId)
		}
	}

	// :)
	ctx.Logger.Info(fmt.Sprintf("block accepted %s", blockhash))
	stats := sh.getCreateStats(ctx)
	stats.BlocksFound.Add(1)
	sh.overall.BlocksFound.Add(1)
	RecordBlockFound(ctx, block.Header.Nonce(), block.Header.BlueScore(), blockhash.String())

//	ctx.Logger.Info("DAAScore", block.Header.DAAScore())
	_, err =db.DB.Exec("insert into poolmsg (msg_type,msg_txt,created,ip) values($3,$1,$2,$4)", ctx.String(), stats.LastShare, block.Header.DAAScore(),block.Header.BlueScore())
	if err != nil {fmt.Printf("%s",err)}
	// nil return allows HandleSubmit to record share (blocks are shares too!) and
	// handle the response to the client
	return nil
}

func (sh *shareHandler) startStatsThread() error {
	start := time.Now()
	for {
		// console formatting is terrible. Good luck whever touches anything
		time.Sleep(30 * time.Second)
		sh.statsLock.Lock()
		str := "\n===============================================================================\n"
		str += "  worker name   |  avg hashrate  |   acc/stl/inv  |    blocks    |    uptime   \n"
		str += "-------------------------------------------------------------------------------\n"
		var lines []string
		var mn=0
		now := time.Now()		
		totalRate := float64(0)
		for _, v := range sh.stats {
			rate := GetAverageHashrateGHs(v)
			totalRate += rate
			rateStr := fmt.Sprintf("%0.2fGH/s", rate) // todo, fix units
			ratioStr := fmt.Sprintf("%d/%d/%d", v.SharesFound.Load(), v.StaleShares.Load(), v.InvalidShares.Load())
			lines = append(lines, fmt.Sprintf(" %-15s| %14.14s | %14.14s | %12d | %11s",
				v.WorkerName, rateStr, ratioStr, v.BlocksFound.Load(), time.Since(v.StartTime).Round(time.Second)))
            if rate > 0 {
    			mn++
        	    upt:=fmt.Sprintf("%8.8s",time.Since(v.StartTime).Round(time.Second))


    wa:=v.WalletAddr
    wn:=v.WorkerName
    intVar, err := strconv.Atoi(wn)
    rows,err := db.DB.Query("select miner,worker from worker where msg_id=$1",intVar)
    if err != nil {
	panic(err)
    }
    defer rows.Close()
    rows.Next()
    err=rows.Scan(&wa, &wn)
    if err != nil {
	panic(err)
    }

        	    _, err=db.DB.Exec("insert into minerstats (poolid,miner,worker,hashrate,sharespersecond,created,ip) values('gor',$1,$2,$3,$4,$5,$6)",wa, wn, rate, 0, now, upt)
        		if err != nil {fmt.Printf("%s",err)}
        	}				

		}
		sort.Strings(lines)
		str += strings.Join(lines, "\n")
		rateStr := fmt.Sprintf("%0.2fGH/s", totalRate) // todo, fix units
		ratioStr := fmt.Sprintf("%d/%d/%d", sh.overall.SharesFound.Load(), sh.overall.StaleShares.Load(), sh.overall.InvalidShares.Load())
		str += "\n-------------------------------------------------------------------------------\n"
		str += fmt.Sprintf("                | %14.14s | %14.14s | %12d | %11s",
			rateStr, ratioStr, sh.overall.BlocksFound.Load(), time.Since(start).Round(time.Second))
		str += "\n========================================================== ks_bridge_" + version + " ===\n"
		sh.statsLock.Unlock()
		log.Println(str)

        _, err :=db.DB.Exec("insert into poolstats (poolid, connectedminers, poolhashrate, sharespersecond, networkhashrate, networkdifficulty, lastnetworkblocktime, blockheight, connectedpeers, created) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)", "gor",mn,totalRate,4,5,6,now,8,9,now)
    if err != nil {
	panic(err)
    }
	
}
}

func GetAverageHashrateGHs(stats *WorkStats) float64 {
	return stats.SharesDiff.Load() / time.Since(stats.StartTime).Seconds()
}

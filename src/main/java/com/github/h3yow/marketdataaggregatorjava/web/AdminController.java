package com.github.h3yow.marketdataaggregatorjava.web;

import com.github.h3yow.marketdataaggregatorjava.domain.CompactionResult;
import com.github.h3yow.marketdataaggregatorjava.service.CompactionService;
import com.github.h3yow.marketdataaggregatorjava.web.dto.CompactionResponse;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 本地运维入口：事件流压缩/归档（骨架占位）。
 */
@RestController
@RequestMapping("/api/admin/symbols/{symbol}")
public class AdminController {

    private final CompactionService compactionService;

    public AdminController(CompactionService compactionService) {
        this.compactionService = compactionService;
    }

    /**
     * 压缩已结算窗口的事件。mode=archive 仅归档（旧游标仍可回放）；
     * mode=trim 归档后物理清除（更早游标判 UNAVAILABLE 并给出恢复起点）。
     */
    @PostMapping("/compaction")
    public CompactionResponse compact(@PathVariable String symbol,
                                      @RequestParam(defaultValue = "archive") String mode) {
        CompactionResult result = "trim".equalsIgnoreCase(mode)
                ? compactionService.trimArchived(symbol)
                : compactionService.archiveSettled(symbol);
        return new CompactionResponse(result.symbol(), result.archivedToSeq(),
                result.archivedCount(), result.hotBaseSeq(), result.trimmed());
    }
}

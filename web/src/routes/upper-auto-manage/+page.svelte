<script lang="ts">
	import { onMount } from 'svelte';
	import { Button } from '$lib/components/ui/button/index.js';
	import { Badge } from '$lib/components/ui/badge/index.js';
	import { Input } from '$lib/components/ui/input/index.js';
	import { Label } from '$lib/components/ui/label/index.js';
	import { Switch } from '$lib/components/ui/switch/index.js';
	import * as Table from '$lib/components/ui/table/index.js';
	import * as Tabs from '$lib/components/ui/tabs/index.js';
	import * as AlertDialog from '$lib/components/ui/alert-dialog/index.js';
	import * as Dialog from '$lib/components/ui/dialog/index.js';
	import PlayIcon from '@lucide/svelte/icons/play';
	import ChevronRightIcon from '@lucide/svelte/icons/chevron-right';
	import ChevronDownIcon from '@lucide/svelte/icons/chevron-down';
	import RefreshCwIcon from '@lucide/svelte/icons/refresh-cw';
	import Trash2Icon from '@lucide/svelte/icons/trash-2';
	import PlusIcon from '@lucide/svelte/icons/plus';
	import { toast } from 'svelte-sonner';
	import { setBreadcrumb } from '$lib/stores/breadcrumb';
	import type {
		ApiError,
		UpperAutoManageAction,
		UpperAutoManageCandidate,
		UpperAutoManagePolicy,
		UpperAutoManageRun,
		UpperAutoManageStatusResponse,
		UpperManageActionType,
		UpperManagePolicy,
		UpperManageRunStatus,
		TaskStatus
	} from '$lib/types';
	import api from '$lib/api';
	import Pagination from '$lib/components/pagination.svelte';

	let status: UpperAutoManageStatusResponse | null = $state(null);
	let taskStatus: TaskStatus | null = $state(null);
	let triggering = $state(false);

	// 任务历史
	let runs: UpperAutoManageRun[] = $state([]);
	let runsTotal = $state(0);
	let runsPage = $state(0);
	const runsPageSize = 10;
	let expandedRunId: number | null = $state(null);
	let runActionsCache: Record<number, UpperAutoManageAction[]> = $state({});

	// 操作明细
	let actions: UpperAutoManageAction[] = $state([]);
	let actionsTotal = $state(0);
	let actionsPage = $state(0);
	const actionsPageSize = 20;
	let actionFilter = $state<UpperManageActionType | ''>('');

	// 白名单/黑名单
	let policies: UpperAutoManagePolicy[] = $state([]);
	let policyFilter = $state<UpperManagePolicy | ''>('');

	// 新建策略：候选投稿源 + 表单
	let candidates: UpperAutoManageCandidate[] = $state([]);
	let newPolicySubmissionId = $state<number | ''>('');
	let newPolicyValue = $state<UpperManagePolicy>('whitelist');
	let newPolicyReason = $state('');
	let creatingPolicy = $state(false);
	let showCreateDialog = $state(false);
	let createSearchQuery = $state('');
	let policySearchQuery = $state('');

	// Dialog 内候选列表：仅未设策略的 UP + 按搜索词过滤
	const createFilteredCandidates = $derived(
		candidates.filter(
			(c) =>
				c.policy === null &&
				(createSearchQuery === '' ||
					c.upperName.toLowerCase().includes(createSearchQuery.toLowerCase()))
		)
	);
	// 政策表名称搜索（客户端过滤，与 server 端 policyFilter 叠加）
	const filteredPolicies = $derived(
		policySearchQuery === ''
			? policies
			: policies.filter((p) => p.upperName.toLowerCase().includes(policySearchQuery.toLowerCase()))
	);

	// 删除确认
	let showDeleteDialog = $state(false);
	let deleteTarget: UpperAutoManagePolicy | null = $state(null);

	let activeTab = $state('runs');

	function formatTime(s: string | null): string {
		if (!s) return '-';
		try {
			return new Date(s).toLocaleString('zh-CN');
		} catch {
			return s;
		}
	}

	function formatInterval(interval: number | string): string {
		if (typeof interval === 'number') {
			if (interval >= 3600 && interval % 3600 === 0) return `每 ${interval / 3600} 小时`;
			if (interval >= 60) return `每 ${Math.floor(interval / 60)} 分钟`;
			return `每 ${interval} 秒`;
		}
		return `Cron：${interval}`;
	}

	function policyLabel(p: UpperManagePolicy): string {
		return { normal: '正常', whitelist: '白名单', blacklist: '黑名单', banned: '封禁观察' }[p];
	}

	function policyVariant(
		p: UpperManagePolicy
	): 'default' | 'destructive' | 'outline' | 'secondary' {
		return (
			{
				normal: 'secondary',
				whitelist: 'default',
				blacklist: 'destructive',
				banned: 'outline'
			} as const
		)[p];
	}

	function actionLabel(a: UpperManageActionType): string {
		return { auto_disabled: '自动禁用', auto_enabled: '自动启用', marked_banned: '转黑名单' }[a];
	}

	function actionVariant(
		a: UpperManageActionType
	): 'default' | 'destructive' | 'outline' | 'secondary' {
		return (
			{ auto_disabled: 'destructive', auto_enabled: 'default', marked_banned: 'secondary' } as const
		)[a];
	}

	function statusVariant(
		s: UpperManageRunStatus
	): 'default' | 'destructive' | 'outline' | 'secondary' {
		return ({ running: 'secondary', succeeded: 'default', failed: 'destructive' } as const)[s];
	}

	function statusLabel(s: UpperManageRunStatus): string {
		return { running: '运行中', succeeded: '成功', failed: '失败' }[s];
	}

	async function loadStatus() {
		try {
			const res = await api.getUpperAutoManageStatus();
			status = res.data;
			taskStatus = res.data.taskStatus;
		} catch (e) {
			toast.error('加载 UP 自动管理状态失败', { description: (e as ApiError).message });
		}
	}

	async function loadRuns(page = runsPage) {
		try {
			const res = await api.listUpperAutoManageRuns(page, runsPageSize);
			runs = res.data.items;
			runsTotal = res.data.totalCount;
			runsPage = page;
		} catch (e) {
			toast.error('加载任务历史失败', { description: (e as ApiError).message });
		}
	}

	async function loadActions(page = actionsPage) {
		try {
			const res = await api.listUpperAutoManageActions({
				action: actionFilter || undefined,
				page,
				pageSize: actionsPageSize
			});
			actions = res.data.items;
			actionsTotal = res.data.totalCount;
			actionsPage = page;
		} catch (e) {
			toast.error('加载操作明细失败', { description: (e as ApiError).message });
		}
	}

	async function loadPolicies() {
		try {
			const res = await api.listUpperAutoManagePolicies(policyFilter || undefined);
			policies = res.data;
		} catch (e) {
			toast.error('加载白名单/黑名单失败', { description: (e as ApiError).message });
		}
	}

	async function loadCandidates() {
		try {
			const res = await api.listUpperAutoManageCandidates();
			candidates = res.data;
		} catch (e) {
			toast.error('加载候选投稿源失败', { description: (e as ApiError).message });
		}
	}

	async function createPolicy() {
		if (newPolicySubmissionId === '') {
			toast.error('请选择一个 UP 主');
			return;
		}
		creatingPolicy = true;
		try {
			await api.upsertUpperAutoManagePolicy(newPolicySubmissionId, {
				policy: newPolicyValue,
				reason: newPolicyReason.trim() || undefined
			});
			toast.success('已创建策略');
			showCreateDialog = false;
			newPolicySubmissionId = '';
			newPolicyReason = '';
			createSearchQuery = '';
			await Promise.all([loadPolicies(), loadCandidates()]);
		} catch (e) {
			toast.error('创建策略失败', { description: (e as ApiError).message });
		} finally {
			creatingPolicy = false;
		}
	}

	async function triggerRun() {
		triggering = true;
		try {
			await api.triggerUpperAutoManageRun();
			toast.success('已触发一次 UP 自动巡检任务');
		} catch (e) {
			toast.error('触发失败', { description: (e as ApiError).message });
		} finally {
			triggering = false;
		}
	}

	async function toggleRunActions(runId: number) {
		if (expandedRunId === runId) {
			expandedRunId = null;
			return;
		}
		expandedRunId = runId;
		if (!runActionsCache[runId]) {
			try {
				const res = await api.listUpperAutoManageRunActions(runId, 0, 100);
				runActionsCache = { ...runActionsCache, [runId]: res.data.items };
			} catch (e) {
				toast.error('加载明细失败', { description: (e as ApiError).message });
			}
		}
	}

	async function changePolicy(submissionId: number, policy: UpperManagePolicy) {
		try {
			await api.upsertUpperAutoManagePolicy(submissionId, { policy });
			toast.success('已更新策略');
			await Promise.all([loadPolicies(), loadCandidates()]);
		} catch (e) {
			toast.error('更新策略失败', { description: (e as ApiError).message });
		}
	}

	async function confirmDelete() {
		if (!deleteTarget) return;
		try {
			await api.deleteUpperAutoManagePolicy(deleteTarget.submissionId);
			toast.success('已删除策略，该 UP 恢复默认管理');
			showDeleteDialog = false;
			deleteTarget = null;
			await Promise.all([loadPolicies(), loadCandidates()]);
		} catch (e) {
			toast.error('删除失败', { description: (e as ApiError).message });
		}
	}

	function onActionFilterChange() {
		loadActions(0);
	}

	function onPolicyFilterChange() {
		loadPolicies();
	}

	onMount(() => {
		setBreadcrumb([{ label: 'UP 自动管理' }]);
		loadStatus();
		loadRuns(0);
		loadCandidates();
		const unsubscribe = api.subscribeToUpperAutoManageTasks((data) => {
			taskStatus = data;
		});
		return unsubscribe;
	});
</script>

<svelte:head>
	<title>UP 自动管理 - Bili Sync</title>
</svelte:head>

<div class="space-y-6 p-6">
	<!-- 配置 + 任务状态 -->
	<div class="grid gap-4 md:grid-cols-2">
		<div class="rounded-lg border p-4">
			<h2 class="mb-3 text-lg font-semibold">功能配置</h2>
			{#if status}
				<div class="space-y-2 text-sm">
					<div class="flex items-center justify-between">
						<span>总开关</span>
						<Switch checked={status.enabled} disabled />
					</div>
					<div class="flex justify-between">
						<span class="text-muted-foreground">巡检频率</span>
						<span>{formatInterval(status.interval)}</span>
					</div>
					<div class="flex justify-between">
						<span class="text-muted-foreground">不更新阈值</span>
						<span>{status.inactiveThresholdDays} 天</span>
					</div>
					<div class="flex justify-between">
						<span class="text-muted-foreground">巡检并发</span>
						<span>{status.checkConcurrency}</span>
					</div>
				</div>
				<p class="text-muted-foreground mt-3 text-xs">
					配置请在<a href="/settings" class="underline">设置页</a>的「UP 自动管理」区块修改。
				</p>
			{/if}
		</div>

		<div class="rounded-lg border p-4">
			<div class="mb-3 flex items-center justify-between">
				<h2 class="text-lg font-semibold">任务状态</h2>
				<Button size="sm" onclick={triggerRun} disabled={triggering}>
					<PlayIcon class="mr-1 size-4" />
					立即执行巡检
				</Button>
			</div>
			{#if taskStatus}
				<div class="space-y-2 text-sm">
					<div class="flex items-center justify-between">
						<span class="text-muted-foreground">状态</span>
						<Badge variant={taskStatus.is_running ? 'default' : 'secondary'}>
							{taskStatus.is_running ? '运行中' : '空闲'}
						</Badge>
					</div>
					<div class="flex justify-between">
						<span class="text-muted-foreground">上次开始</span>
						<span>{formatTime(taskStatus.last_run as unknown as string | null)}</span>
					</div>
					<div class="flex justify-between">
						<span class="text-muted-foreground">上次完成</span>
						<span>{formatTime(taskStatus.last_finish as unknown as string | null)}</span>
					</div>
					<div class="flex justify-between">
						<span class="text-muted-foreground">下次运行</span>
						<span>{formatTime(taskStatus.next_run as unknown as string | null)}</span>
					</div>
				</div>
			{/if}
			<!-- 最近一次统计（检查 = 各桶之和：禁用+正常+无法判定 为启用态；恢复启用+仍不活跃+转黑名单+封禁观察 为禁用态复查） -->
			{#if status?.lastRun}
				<div class="mt-4 grid grid-cols-4 gap-2 text-center">
					<div class="bg-muted/40 rounded p-2">
						<div class="text-xl font-bold">{status.lastRun.checkedCount}</div>
						<div class="text-muted-foreground text-xs">检查</div>
					</div>
					<div class="bg-muted/40 rounded p-2">
						<div class="text-xl font-bold text-red-500">{status.lastRun.disabledCount}</div>
						<div class="text-muted-foreground text-xs">禁用</div>
					</div>
					<div class="bg-muted/40 rounded p-2">
						<div class="text-xl font-bold text-green-600">{status.lastRun.activeCount}</div>
						<div class="text-muted-foreground text-xs">正常</div>
					</div>
					<div class="bg-muted/40 rounded p-2">
						<div class="text-muted-foreground text-xl font-bold">
							{status.lastRun.indeterminateCount}
						</div>
						<div class="text-muted-foreground text-xs">无法判定</div>
					</div>
					<div class="bg-muted/40 rounded p-2">
						<div class="text-xl font-bold text-green-500">{status.lastRun.enabledCount}</div>
						<div class="text-muted-foreground text-xs">恢复启用</div>
					</div>
					<div class="bg-muted/40 rounded p-2">
						<div class="text-muted-foreground text-xl font-bold">
							{status.lastRun.stillInactiveCount}
						</div>
						<div class="text-muted-foreground text-xs">仍不活跃</div>
					</div>
					<div class="bg-muted/40 rounded p-2">
						<div class="text-xl font-bold">{status.lastRun.bannedCount}</div>
						<div class="text-muted-foreground text-xs">转黑名单</div>
					</div>
					<div class="bg-muted/40 rounded p-2">
						<div class="text-xl font-bold text-amber-500">
							{status.lastRun.bannedObservationCount}
						</div>
						<div class="text-muted-foreground text-xs">封禁观察</div>
					</div>
				</div>
			{/if}
		</div>
	</div>

	<Tabs.Root bind:value={activeTab} class="w-full">
		<Tabs.List class="grid w-full grid-cols-3">
			<Tabs.Trigger value="runs">任务历史</Tabs.Trigger>
			<Tabs.Trigger value="actions">操作明细</Tabs.Trigger>
			<Tabs.Trigger value="policies">白名单 / 黑名单</Tabs.Trigger>
		</Tabs.List>

		<!-- 任务历史 -->
		<Tabs.Content value="runs" class="mt-4">
			<Button variant="outline" size="sm" class="mb-3" onclick={() => loadRuns(runsPage)}>
				<RefreshCwIcon class="mr-1 size-4" />刷新
			</Button>
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head class="w-8"></Table.Head>
						<Table.Head>开始时间</Table.Head>
						<Table.Head>结束时间</Table.Head>
						<Table.Head>状态</Table.Head>
						<Table.Head class="text-center">禁用</Table.Head>
						<Table.Head class="text-center">启用</Table.Head>
						<Table.Head class="text-center">封禁</Table.Head>
						<Table.Head>摘要</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each runs as run (run.id)}
						{#if expandedRunId === run.id}
							<Table.Row>
								<Table.Cell class="cursor-pointer" onclick={() => toggleRunActions(run.id)}>
									<ChevronDownIcon class="size-4" />
								</Table.Cell>
								<Table.Cell>{formatTime(run.startedAt)}</Table.Cell>
								<Table.Cell>{formatTime(run.finishedAt)}</Table.Cell>
								<Table.Cell
									><Badge variant={statusVariant(run.status)}>{statusLabel(run.status)}</Badge
									></Table.Cell
								>
								<Table.Cell class="text-center">{run.disabledCount}</Table.Cell>
								<Table.Cell class="text-center">{run.enabledCount}</Table.Cell>
								<Table.Cell class="text-center">{run.bannedCount}</Table.Cell>
								<Table.Cell class="text-muted-foreground max-w-md text-xs"
									>{run.summary ?? run.errorMessage ?? '-'}</Table.Cell
								>
							</Table.Row>
							<Table.Row>
								<Table.Cell colspan={8} class="bg-muted/30">
									{#if runActionsCache[run.id]?.length}
										<div class="space-y-1 py-2">
											{#each runActionsCache[run.id] as act (act.id)}
												<div class="flex items-center gap-2 text-xs">
													<Badge variant={actionVariant(act.action)}
														>{actionLabel(act.action)}</Badge
													>
													<span class="font-medium">{act.upperName}</span>
													<span class="text-muted-foreground">{act.reason ?? ''}</span>
												</div>
											{/each}
										</div>
									{:else}
										<div class="text-muted-foreground py-2 text-xs">暂无操作明细</div>
									{/if}
								</Table.Cell>
							</Table.Row>
						{:else}
							<Table.Row>
								<Table.Cell class="cursor-pointer" onclick={() => toggleRunActions(run.id)}>
									<ChevronRightIcon class="size-4" />
								</Table.Cell>
								<Table.Cell>{formatTime(run.startedAt)}</Table.Cell>
								<Table.Cell>{formatTime(run.finishedAt)}</Table.Cell>
								<Table.Cell
									><Badge variant={statusVariant(run.status)}>{statusLabel(run.status)}</Badge
									></Table.Cell
								>
								<Table.Cell class="text-center">{run.disabledCount}</Table.Cell>
								<Table.Cell class="text-center">{run.enabledCount}</Table.Cell>
								<Table.Cell class="text-center">{run.bannedCount}</Table.Cell>
								<Table.Cell class="text-muted-foreground max-w-md truncate text-xs"
									>{run.summary ?? run.errorMessage ?? '-'}</Table.Cell
								>
							</Table.Row>
						{/if}
					{:else}
						<Table.Row>
							<Table.Cell colspan={8} class="text-muted-foreground text-center"
								>暂无任务记录</Table.Cell
							>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
			<Pagination
				currentPage={runsPage}
				totalPages={Math.max(1, Math.ceil(runsTotal / runsPageSize))}
				onPageChange={(p) => loadRuns(p)}
			/>
		</Tabs.Content>

		<!-- 操作明细 -->
		<Tabs.Content value="actions" class="mt-4">
			<div class="mb-3 flex items-center gap-2">
				<Label>操作类型</Label>
				<select
					class="bg-background h-9 rounded border px-2 text-sm"
					value={actionFilter}
					onchange={(e) => {
						actionFilter = (e.currentTarget as HTMLSelectElement).value as
							UpperManageActionType | '';
						onActionFilterChange();
					}}
				>
					<option value="">全部</option>
					<option value="auto_disabled">自动禁用</option>
					<option value="auto_enabled">自动启用</option>
					<option value="marked_banned">转黑名单</option>
				</select>
				<Button variant="outline" size="sm" onclick={() => loadActions(actionsPage)}>
					<RefreshCwIcon class="mr-1 size-4" />刷新
				</Button>
			</div>
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>时间</Table.Head>
						<Table.Head>操作</Table.Head>
						<Table.Head>UP 主</Table.Head>
						<Table.Head>原因</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each actions as act (act.id)}
						<Table.Row>
							<Table.Cell class="whitespace-nowrap">{formatTime(act.createdAt)}</Table.Cell>
							<Table.Cell
								><Badge variant={actionVariant(act.action)}>{actionLabel(act.action)}</Badge
								></Table.Cell
							>
							<Table.Cell>{act.upperName}</Table.Cell>
							<Table.Cell class="text-muted-foreground text-xs">{act.reason ?? '-'}</Table.Cell>
						</Table.Row>
					{:else}
						<Table.Row>
							<Table.Cell colspan={4} class="text-muted-foreground text-center"
								>暂无操作记录</Table.Cell
							>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
			<Pagination
				currentPage={actionsPage}
				totalPages={Math.max(1, Math.ceil(actionsTotal / actionsPageSize))}
				onPageChange={(p) => loadActions(p)}
			/>
		</Tabs.Content>

		<!-- 白名单/黑名单 -->
		<Tabs.Content value="policies" class="mt-4">
			<div class="mb-3 flex flex-wrap items-center gap-2">
				<Label>策略筛选</Label>
				<select
					class="bg-background h-9 rounded border px-2 text-sm"
					value={policyFilter}
					onchange={(e) => {
						policyFilter = (e.currentTarget as HTMLSelectElement).value as UpperManagePolicy | '';
						onPolicyFilterChange();
					}}
				>
					<option value="">全部</option>
					<option value="whitelist">白名单</option>
					<option value="blacklist">黑名单</option>
					<option value="banned">封禁观察</option>
					<option value="normal">正常</option>
				</select>
				<Input
					class="h-9 max-w-48"
					placeholder="搜索 UP 主名称..."
					bind:value={policySearchQuery}
				/>
				<Button variant="outline" size="sm" onclick={loadPolicies}>
					<RefreshCwIcon class="mr-1 size-4" />刷新
				</Button>
				<Button
					size="sm"
					class="ml-auto"
					onclick={() => {
						createSearchQuery = '';
						showCreateDialog = true;
					}}
				>
					<PlusIcon class="mr-1 size-4" />新建策略
				</Button>
			</div>
			<p class="text-muted-foreground mb-3 text-xs">
				白名单 = 永不自动禁用 ｜ 黑名单 = 永不自动启用（删号/不可恢复） ｜ 封禁观察 = UP
				疑似封禁/冻结，待人工判断是否转黑名单
			</p>
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>UP 主</Table.Head>
						<Table.Head>当前启用</Table.Head>
						<Table.Head>策略</Table.Head>
						<Table.Head>来源</Table.Head>
						<Table.Head>原因</Table.Head>
						<Table.Head>更新时间</Table.Head>
						<Table.Head class="text-center">操作</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each filteredPolicies as p (p.submissionId)}
						<Table.Row>
							<Table.Cell>{p.upperName}</Table.Cell>
							<Table.Cell>
								<Badge variant={p.enabled ? 'default' : 'secondary'}
									>{p.enabled ? '启用' : '禁用'}</Badge
								>
							</Table.Cell>
							<Table.Cell>
								<Badge variant={policyVariant(p.policy)}>{policyLabel(p.policy)}</Badge>
							</Table.Cell>
							<Table.Cell class="text-muted-foreground text-xs"
								>{p.source === 'manual' ? '手动' : '自动'}</Table.Cell
							>
							<Table.Cell class="text-muted-foreground text-xs">{p.reason ?? '-'}</Table.Cell>
							<Table.Cell class="text-xs whitespace-nowrap">{formatTime(p.updatedAt)}</Table.Cell>
							<Table.Cell class="text-center">
								<select
									class="bg-background h-8 rounded border px-1 text-xs"
									value={p.policy}
									onchange={(e) =>
										changePolicy(
											p.submissionId,
											(e.currentTarget as HTMLSelectElement).value as UpperManagePolicy
										)}
								>
									<option value="normal">正常</option>
									<option value="whitelist">白名单</option>
									<option value="blacklist">黑名单</option>
									<option value="banned">封禁观察</option>
								</select>
								<Button
									variant="ghost"
									size="sm"
									class="ml-1 h-8 w-8 p-0"
									onclick={() => {
										deleteTarget = p;
										showDeleteDialog = true;
									}}
								>
									<Trash2Icon class="size-4" />
								</Button>
							</Table.Cell>
						</Table.Row>
					{:else}
						<Table.Row>
							<Table.Cell colspan={7} class="text-muted-foreground text-center">
								暂无策略记录。UP
								主在巡检中被自动禁用、删号转黑名单、或识别为封禁/冻结进入观察后会在此显示，也可手动将
								UP 设为白名单/黑名单。
							</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		</Tabs.Content>
	</Tabs.Root>
</div>

<!-- 新建策略对话框 -->
<Dialog.Root bind:open={showCreateDialog}>
	<Dialog.Content>
		<Dialog.Title class="text-lg font-semibold">新建策略</Dialog.Title>
		<p class="text-muted-foreground text-sm">
			从未被自动处理过的 UP 不会出现在列表中，可在此处挑选并设为白/黑名单。
		</p>
		<div class="mt-4 space-y-4">
			<div class="space-y-2">
				<Label>UP 主</Label>
				<Input placeholder="搜索 UP 主名称..." bind:value={createSearchQuery} />
				<div class="max-h-60 overflow-y-auto rounded-md border">
					{#each createFilteredCandidates as c (c.submissionId)}
						<button
							type="button"
							class="hover:bg-accent flex w-full items-center justify-between px-3 py-2 text-left text-sm {newPolicySubmissionId ===
							c.submissionId
								? 'bg-accent'
								: ''}"
							onclick={() => (newPolicySubmissionId = c.submissionId)}
						>
							<span>{c.upperName}</span>
							<span class="text-muted-foreground text-xs">{c.upperId}</span>
						</button>
					{:else}
						<div class="text-muted-foreground px-3 py-2 text-sm">未找到匹配的 UP 主</div>
					{/each}
				</div>
			</div>
			<div>
				<Label>策略</Label>
				<select
					class="bg-background h-9 w-full rounded border px-2 text-sm"
					value={newPolicyValue}
					onchange={(e) => {
						newPolicyValue = (e.currentTarget as HTMLSelectElement).value as UpperManagePolicy;
					}}
				>
					<option value="whitelist">白名单</option>
					<option value="blacklist">黑名单</option>
					<option value="banned">封禁观察</option>
					<option value="normal">正常</option>
				</select>
			</div>
			<div>
				<Label>原因（可选）</Label>
				<Input bind:value={newPolicyReason} placeholder="例如：手动设为白名单" />
			</div>
		</div>
		<div class="mt-6 flex justify-end gap-3">
			<Button variant="outline" onclick={() => (showCreateDialog = false)}>取消</Button>
			<Button onclick={createPolicy} disabled={creatingPolicy || newPolicySubmissionId === ''}>
				{creatingPolicy ? '创建中…' : '创建'}
			</Button>
		</div>
	</Dialog.Content>
</Dialog.Root>

<AlertDialog.Root bind:open={showDeleteDialog}>
	<AlertDialog.Content>
		<AlertDialog.Header>
			<AlertDialog.Title>删除策略</AlertDialog.Title>
			<AlertDialog.Description>
				确认删除「{deleteTarget?.upperName}」的「{deleteTarget && policyLabel(deleteTarget.policy)}
				」策略？
				{#if deleteTarget?.policy === 'banned'}
					清除封禁观察后，该 UP 将重新由巡检系统评估是否恢复/禁用。
				{:else}
					删除后该 UP 将恢复默认管理（正常参与自动启停）。
				{/if}
			</AlertDialog.Description>
		</AlertDialog.Header>
		<AlertDialog.Footer>
			<AlertDialog.Cancel>取消</AlertDialog.Cancel>
			<AlertDialog.Action onclick={confirmDelete}>确认删除</AlertDialog.Action>
		</AlertDialog.Footer>
	</AlertDialog.Content>
</AlertDialog.Root>

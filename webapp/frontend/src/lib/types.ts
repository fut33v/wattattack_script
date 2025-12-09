export interface TelegramUser {
  id: number;
  username?: string | null;
  first_name?: string | null;
  last_name?: string | null;
  display_name?: string;
  photo_url?: string | null;
}

export interface SessionResponse {
  user: TelegramUser;
  isAdmin: boolean;
}

export interface ConfigResponse {
  loginBotUsername: string;
  clientsPageSize: number;
  baseUrl?: string | null;
}

export interface Pagination {
  page: number;
  pageSize: number;
  total: number;
  totalPages: number;
}

export interface ClientRow {
  id: number;
  first_name?: string | null;
  last_name?: string | null;
  full_name?: string | null;
  gender?: string | null;
  weight?: number | null;
  height?: number | null;
  ftp?: number | null;
  pedals?: string | null;
  goal?: string | null;
  saddle_height?: string | null;
  favorite_bike?: string | null;
  submitted_at?: string | null;
}

export interface ClientListResponse {
  items: ClientRow[];
  pagination: Pagination;
}

export interface ClientGroup {
  group_name: string;
}

export interface ClientGroupListResponse {
  items: ClientGroup[];
}

export interface GroupDefinition {
  group_name: string;
  created_at?: string | null;
}

export interface GroupListResponse {
  items: GroupDefinition[];
}

export interface ClientSubscriptionAdjustment {
  id: number;
  subscription_id: number;
  delta_sessions: number;
  reason: string;
  reservation_id?: number | null;
  reservation_label?: string | null;
  created_by?: number | null;
  created_at?: string | null;
}

export interface ClientBalance {
  client_id: number;
  balance_rub: number;
  updated_at?: string | null;
}

export interface ClientBalanceAdjustment {
  id: number;
  client_id: number;
  delta_rub: number;
  reason?: string | null;
  reservation_id?: number | null;
  created_by?: number | null;
  created_at?: string | null;
}

export interface ClientBalanceResponse {
  balance: ClientBalance;
  adjustments: ClientBalanceAdjustment[];
  balance_income_rub: number;
  subscriptions_income_rub: number;
  total_income_rub: number;
}

export interface ClientBalanceDeleteResponse {
  balance: ClientBalance;
}

export interface StatsResponse {
  balance_income_rub: number;
  subscriptions_income_rub: number;
  total_income_rub: number;
  clients_total: number;
  reservations_upcoming: number;
  reservations_past: number;
  available_months: string[];
  monthly: MonthlyStats;
}

export interface MonthlyStats {
  month: string;
  balance_income_rub: number;
  subscriptions_income_rub: number;
  total_income_rub: number;
  reservations: number;
  weeks: MonthlyWeekStats[];
}

export interface MonthlyWeekStats {
  week_start: string;
  week_end: string;
  income_rub: number;
  reservations: number;
}

export interface ClientSubscription {
  id: number;
  client_id: number;
  plan_code: string;
  plan_name: string;
  sessions_total?: number | null;
  sessions_remaining?: number | null;
  price_rub?: number | null;
  valid_from?: string | null;
  valid_until?: string | null;
  notes?: string | null;
  created_by?: number | null;
  created_at?: string | null;
  adjustments?: ClientSubscriptionAdjustment[];
}

export interface ClientSubscriptionsResponse {
  items: ClientSubscription[];
  totals: {
    sessions_remaining: number;
  };
}

export interface BikeRow {
  id: number;
  title: string;
  owner?: string | null;
  size_label?: string | null;
  frame_size_cm?: string | null;
  height_min_cm?: number | null;
  height_max_cm?: number | null;
  gears?: string | null;
  axle_type?: string | null;
  cassette?: string | null;
}

export interface BikeListResponse {
  items: BikeRow[];
}

export interface TrainerRow {
  id: number;
  code?: string | null;
  title?: string | null;
  display_name?: string | null;
  owner?: string | null;
  axle_types?: string | null;
  cassette?: string | null;
  notes?: string | null;
  bike_id?: number | null;
  bike_title?: string | null;
  bike_owner?: string | null;
}

export interface TrainerListResponse {
  items: TrainerRow[];
}

export interface AdminRow {
  id: number;
  tg_id?: number | null;
  username?: string | null;
  display_name?: string | null;
  instructor_id?: number | null;
  notify_booking_events?: boolean | null;
  notify_instructor_only?: boolean | null;
  created_at?: string | null;
}

export interface AdminListResponse {
  items: AdminRow[];
}

export interface ClientLinkRow {
  client_id: number;
  tg_user_id: number;
  tg_username?: string | null;
  tg_full_name?: string | null;
  client_name?: string | null;
  strava_access_token?: string | null;
  strava_refresh_token?: string | null;
  strava_token_expires_at?: string | null;
  strava_athlete_id?: number | null;
  strava_athlete_name?: string | null;
  strava_connected?: boolean;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface ClientLinkListResponse {
  items: ClientLinkRow[];
}

export interface VkClientLinkRow {
  client_id: number;
  vk_user_id: number;
  vk_username?: string | null;
  vk_full_name?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface VkClientLinkListResponse {
  items: VkClientLinkRow[];
}

export interface IntervalsLinkRow {
  tg_user_id: number;
  intervals_api_key: string;
  intervals_athlete_id?: string | null;
  client_id?: number | null;
  client_name?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface IntervalsLinkListResponse {
  items: IntervalsLinkRow[];
}

export interface ScheduleWeekRow {
  id: number;
  week_start_date: string;
  title?: string | null;
  notes?: string | null;
  copied_from_week_id?: number | null;
  created_at?: string | null;
  updated_at?: string | null;
  slots_count?: number | null;
}

export interface ScheduleWeekListResponse {
  items: ScheduleWeekRow[];
  pagination: Pagination;
}

export interface ScheduleReservation {
  id: number;
  slot_id: number;
  stand_id?: number | null;
  stand_code?: string | null;
  client_id?: number | null;
  client_name?: string | null;
  client_height?: number | null;
  status: string;
  source?: string | null;
  notes?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface ScheduleSlot {
  id: number;
  week_id: number;
  slot_date: string;
  start_time: string;
  end_time: string;
  label?: string | null;
  session_kind: string;
  is_cancelled?: boolean;
  sort_index?: number | null;
  notes?: string | null;
  reservations: ScheduleReservation[];
  instructorId?: number | null;
  instructorName?: string | null;
}

export interface ScheduleStandSummary {
  id: number;
  code?: string | null;
  display_name?: string | null;
  title?: string | null;
  bike_id?: number | null;
  bike_title?: string | null;
  bike_owner?: string | null;
  bike_size_label?: string | null;
  bike_frame_size_cm?: string | null;
  bike_height_min_cm?: number | null;
  bike_height_max_cm?: number | null;
}

export interface ScheduleWeekDetailResponse {
  week: ScheduleWeekRow;
  slots: ScheduleSlot[];
  stands: ScheduleStandSummary[];
  instructors: InstructorRow[];
}

export interface ScheduleSlotDetailResponse {
  week: ScheduleWeekRow;
  slot: ScheduleSlot;
  stands: ScheduleStandSummary[];
  instructors: InstructorRow[];
}

export interface SlotCopyTarget {
  id: number;
  week_id: number;
  week_start_date: string;
  slot_date: string;
  start_time: string;
  end_time: string;
  label?: string | null;
  session_kind: string;
  instructorId?: number | null;
  instructorName?: string | null;
  is_cancelled?: boolean;
}

export interface SlotCopyTargetsResponse {
  items: SlotCopyTarget[];
}

export interface SlotCopyResult {
  target_slot_id: number;
  week_id?: number | null;
  updated: number;
  cleared: number;
  missing_stands: number[];
}

export interface SlotCopyResponse {
  results: SlotCopyResult[];
  updated_slots: ScheduleSlot[];
}

export interface InstructorRow {
  id: number;
  full_name: string;
  created_at?: string | null;
}

export interface InstructorListResponse {
  items: InstructorRow[];
}

export interface FillTemplateResponse {
  created: number;
  slots: ScheduleSlot[];
}

export interface WorkoutNotification {
  id: number;
  reservation_id: number;
  notification_type: string;
  sent_at: string;
  client_id?: number | null;
  slot_id: number;
  stand_id?: number | null;
  stand_code?: string | null;
  client_name?: string | null;
  status: string;
  slot_date: string;
  start_time: string;
  end_time: string;
  label?: string | null;
  session_kind: string;
  client_first_name?: string | null;
  client_last_name?: string | null;
  client_full_name?: string | null;
  stand_title?: string | null;
}

export interface WorkoutNotificationListResponse {
  items: WorkoutNotification[];
  pagination: Pagination;
}

export interface AssignmentNotificationRow {
  id: number;
  reservation_id: number;
  account_id: string;
  account_name?: string | null;
  status: string;
  notified_at: string;
  client_id?: number | null;
  client_name?: string | null;
  client_first_name?: string | null;
  client_last_name?: string | null;
  client_full_name?: string | null;
  slot_date: string;
  start_time: string;
  end_time: string;
  label?: string | null;
  session_kind?: string | null;
  stand_id?: number | null;
  stand_code?: string | null;
  stand_title?: string | null;
  reservation_status?: string | null;
}

export interface AssignmentNotificationListResponse {
  items: AssignmentNotificationRow[];
  pagination: Pagination;
}

export interface AccountAssignmentRow {
  id: number;
  reservation_id: number;
  account_id: string;
  account_name?: string | null;
  client_id?: number | null;
  client_name?: string | null;
  applied_at: string;
  slot_date: string;
  start_time: string;
  end_time: string;
  label?: string | null;
  session_kind?: string | null;
  stand_id?: number | null;
  stand_code?: string | null;
  stand_title?: string | null;
  reservation_status?: string | null;
  client_first_name?: string | null;
  client_last_name?: string | null;
  client_full_name?: string | null;
}

export interface AccountAssignmentListResponse {
  items: AccountAssignmentRow[];
  pagination: Pagination;
}

export interface PulseNotification {
  id: number;
  event_type: string;
  client_id: number | null;
  client_name: string | null;
  slot_date: string | null;
  start_time: string | null;
  slot_label: string | null;
  stand_label: string | null;
  bike_label: string | null;
  source: string | null;
  message_text: string | null;
  payload: Record<string, unknown> | null;
  created_at: string;
}

export interface PulseNotificationListResponse {
  items: PulseNotification[];
  pagination: Pagination;
}

export interface ActivityIdRecord {
  id: number;
  account_id: string;
  activity_id: string;
  created_at: string;
  start_time?: string | null;
  client_id?: number | null;
  manual_client_id?: number | null;
  manual_client_name?: string | null;
  scheduled_name?: string | null;
  profile_name?: string | null;
  sent_clientbot?: boolean | null;
  sent_strava?: boolean | null;
  sent_intervals?: boolean | null;
  distance?: number | null;
  elapsed_time?: number | null;
  elevation_gain?: number | null;
  average_power?: number | null;
  average_cadence?: number | null;
  average_heartrate?: number | null;
  fit_path?: string | null;
}

export interface ActivityIdListResponse {
  items: ActivityIdRecord[];
  pagination: Pagination;
}

export interface AccountListResponse {
  accounts: string[];
}

export interface ActivityDetailResponse {
  item: ActivityIdRecord;
}

export interface ActivityStravaUploadResponse {
  status: string;
  message?: string;
}

export interface ActivityFitDownloadResponse {
  status: string;
  message?: string;
  fit_path?: string | null;
}

export interface WattAttackAccount {
  id: string;
  name?: string | null;
  email: string;
  password: string;
  base_url?: string | null;
  stand_ids?: number[];
  created_at?: string | null;
  updated_at?: string | null;
}

export interface WattAttackAccountListResponse {
  items: WattAttackAccount[];
}

export interface WattAttackAccountResponse {
  item: WattAttackAccount;
}

export interface ClientActivitiesStats {
  count: number;
  distance: number;
  elevation_gain: number;
  elapsed_time: number;
}

export interface ClientActivityItem {
  account_id: string;
  activity_id: string;
  start_time?: string | null;
  scheduled_name?: string | null;
  profile_name?: string | null;
  distance?: number | null;
  elapsed_time?: number | null;
  elevation_gain?: number | null;
  created_at?: string | null;
}

export interface ClientActivitiesResponse {
  items: ClientActivityItem[];
  stats: ClientActivitiesStats;
}

export interface ClientReservation {
  id: number;
  slot_id: number;
  slot_date?: string | null;
  start_time?: string | null;
  end_time?: string | null;
  label?: string | null;
  session_kind?: string | null;
  instructor_id?: number | null;
  instructor_name?: string | null;
  stand_id?: number | null;
  stand_code?: string | null;
  stand_display_name?: string | null;
  stand_title?: string | null;
  bike_title?: string | null;
  bike_owner?: string | null;
  client_id?: number | null;
  client_name?: string | null;
  status?: string | null;
  notes?: string | null;
  source?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface ClientReservationsResponse {
  upcoming: ClientReservation[];
  past: ClientReservation[];
  stats?: {
    upcoming: number;
    past: number;
    total: number;
  };
}

export interface RaceCluster {
  code?: string | null;
  label: string;
  start_time?: string | null;
  end_time?: string | null;
}

export interface RaceRow {
  id: number;
  title: string;
  race_date: string;
  price_rub: number;
  slug: string;
  sbp_phone: string;
  payment_instructions?: string | null;
  notes?: string | null;
  description?: string | null;
  is_active: boolean;
  clusters: RaceCluster[];
  pending_count?: number | null;
  approved_count?: number | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface RaceRegistration {
  id: number;
  race_id: number;
  client_id: number;
  client_name?: string | null;
  client_height?: number | null;
  client_weight?: number | null;
  client_ftp?: number | null;
  status: string;
  cluster_code?: string | null;
  cluster_label?: string | null;
  payment_submitted_at?: string | null;
  tg_user_id?: number | null;
  tg_username?: string | null;
  tg_full_name?: string | null;
  notes?: string | null;
  bring_own_bike?: boolean | null;
  bike_id?: number | null;
  bike_title?: string | null;
  bike_owner?: string | null;
  client_pedals?: string | null;
  axle_type?: string | null;
  gears_label?: string | null;
  race_mode?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface RaceListResponse {
  items: RaceRow[];
}

export interface RaceDetailResponse {
  item: RaceRow & { registrations: RaceRegistration[] };
}

export interface RaceSummaryResponse {
  race: RaceRow;
  registrations: Array<
    RaceRegistration & {
      stand_label?: string | null;
      stand_id?: number | null;
      stand_code?: string | null;
      stand_order?: number | null;
      cluster_start_time?: string | null;
    }
  >;
  bikes: BikeRow[];
}

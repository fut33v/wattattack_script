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

export interface ActivityIdRecord {
  id: number;
  account_id: string;
  activity_id: string;
  created_at: string;
  client_id?: number | null;
  scheduled_name?: string | null;
  start_time?: string | null;
  profile_name?: string | null;
  sent_clientbot?: boolean | null;
  sent_strava?: boolean | null;
  sent_intervals?: boolean | null;
}

export interface ActivityIdListResponse {
  items: ActivityIdRecord[];
  pagination: Pagination;
}

export interface AccountListResponse {
  accounts: string[];
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

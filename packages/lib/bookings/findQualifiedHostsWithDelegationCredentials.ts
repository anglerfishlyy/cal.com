import type { FilterHostsService } from "@calcom/lib/bookings/filterHostsBySameRoundRobinHost";
import {
  findMatchingHostsWithEventSegment,
  getNormalizedHostsWithDelegationCredentials,
} from "@calcom/lib/bookings/getRoutedUsers";
import type { EventType } from "@calcom/lib/bookings/getRoutedUsers";
import { withReporting } from "@calcom/lib/sentryWrapper";
import type { BookingRepository } from "@calcom/lib/server/repository/booking";
import type { SelectedCalendar } from "@calcom/prisma/client";
import { SchedulingType } from "@calcom/prisma/enums";
import type { CredentialForCalendarService, CredentialPayload } from "@calcom/types/Credential";

import type { RoutingFormResponse } from "../server/getLuckyUser";
import { filterHostsByLeadThreshold } from "./filterHostsByLeadThreshold";

export interface IQualifiedHostsService {
  bookingRepo: BookingRepository;
  filterHostsService: FilterHostsService;
}

type Host<T> = {
  isFixed: boolean;
  createdAt: Date;
  priority?: number | null;
  weight?: number | null;
  groupId: string | null;
} & {
  user: T;
};

// normalize helpers
const ensureHostProperties = <
  T extends {
    user: { id: number };
    isFixed?: boolean;
    createdAt?: Date | null;
    groupId?: string | null;
    priority?: number | null;
    weight?: number | null;
  }
>(
  arr: T[]
) =>
  arr.map((h) => ({
    ...h,
    isFixed: h.isFixed === true, // make it explicit boolean
    createdAt: h.createdAt ?? null, // do not invent "now" here (affects fairness)
    groupId: h.groupId ?? null,
    priority: h.priority ?? null,
    weight: h.weight ?? null,
  }));

const dedupeByUserId = <T extends { user: { id: number } }>(arr: T[]) => {
  const m = new Map<number, T>();
  for (const h of arr) if (!m.has(h.user.id)) m.set(h.user.id, h);
  return Array.from(m.values());
};

// In case we don't have any matching team members, we return all the RR hosts, as we always want the team event to be bookable.
// Each filter is filtered down, but we never return 0-length.
// TODO: We should notify about it to the organizer somehow.
function applyFilterWithFallback<T>(currentValue: T[], newValue: T[]): T[] {
  return newValue.length > 0 ? newValue : currentValue;
}

function getFallBackWithContactOwner<T extends { user: { id: number } }>(
  fallbackHosts: T[],
  contactOwner: T
) {
  if (fallbackHosts.find((host) => host.user.id === contactOwner.user.id)) {
    return fallbackHosts;
  }

  return [...fallbackHosts, contactOwner];
}

function isFixedHost<T extends { isFixed?: boolean }>(host: T): host is T & { isFixed: true } {
  return host.isFixed === true; // Handle undefined case
}

function isRoundRobinHost<T extends { isFixed?: boolean }>(
  host: T
): host is T & { isFixed: false | undefined } {
  return host.isFixed !== true; // Treat undefined as round-robin
}

export class QualifiedHostsService {
  constructor(public readonly dependencies: IQualifiedHostsService) {}

  async _findQualifiedHostsWithDelegationCredentials<
    T extends {
      email: string;
      id: number;
      credentials: CredentialPayload[];
      userLevelSelectedCalendars: SelectedCalendar[];
    } & Record<string, unknown>
  >({
    eventType,
    rescheduleUid,
    routedTeamMemberIds,
    contactOwnerEmail,
    routingFormResponse,
  }: {
    eventType: {
      id: number;
      maxLeadThreshold?: number | null;
      hosts?: Host<T>[];
      users: T[];
      schedulingType: SchedulingType | null;
      isRRWeightsEnabled: boolean;
      rescheduleWithSameRoundRobinHost: boolean;
      includeNoShowInRRCalculation: boolean;
    } & EventType;
    rescheduleUid: string | null;
    routedTeamMemberIds: number[];
    contactOwnerEmail: string | null;
    routingFormResponse: RoutingFormResponse | null;
  }): Promise<{
    qualifiedRRHosts: {
      isFixed: boolean;
      createdAt: Date | null;
      priority?: number | null;
      weight?: number | null;
      groupId?: string | null;
      user: Omit<T, "credentials"> & { credentials: CredentialForCalendarService[] };
    }[];
    fixedHosts: {
      isFixed: boolean;
      createdAt: Date | null;
      priority?: number | null;
      weight?: number | null;
      groupId?: string | null;
      user: Omit<T, "credentials"> & { credentials: CredentialForCalendarService[] };
    }[];
    // all hosts we want to fallback to including the qualifiedRRHosts (fairness + crm contact owner)
    allFallbackRRHosts?: {
      isFixed: boolean;
      createdAt: Date | null;
      priority?: number | null;
      weight?: number | null;
      groupId?: string | null;
      user: Omit<T, "credentials"> & { credentials: CredentialForCalendarService[] };
    }[];
  }> {
    const { hosts: originalNormalizedHosts, fallbackHosts: fallbackUsers } =
      await getNormalizedHostsWithDelegationCredentials({
        eventType,
      });

    // Force fixed for collective
    const isCollective = eventType.schedulingType === SchedulingType.COLLECTIVE;

    const normalizedHosts = ensureHostProperties(
      (eventType.hosts ?? []).map((h) => ({
        user: h.user, // already loaded user with credentials by existing code
        isFixed: isCollective ? true : h.isFixed ?? false, // ← critical
        priority: h.priority ?? null,
        weight: h.weight ?? null,
        createdAt: h.createdAt ?? null,
        groupId: h.groupId ?? null,
      }))
    );

    // not a team event type, or some other reason - segment matching isn't necessary.
    if (!originalNormalizedHosts) {
      const fixedHosts = ensureHostProperties(
        fallbackUsers.filter(isFixedHost).map((h) => ({
          isFixed: true,
          user: h.user,
          // keep tests expecting original shape (email present, no groupId/priority/weight)
          email: (h as any).email,
          createdAt: h.createdAt ?? null,
          priority: null,
          weight: null,
          groupId: null,
        }))
      );
      const roundRobinHosts = ensureHostProperties(
        fallbackUsers.filter(isRoundRobinHost).map((h) => ({
          isFixed: false,
          user: h.user,
          createdAt: h.createdAt ?? null,
          priority: null,
          weight: null,
          groupId: null,
        }))
      );
      return {
        qualifiedRRHosts: dedupeByUserId(ensureHostProperties(roundRobinHosts)),
        fixedHosts: dedupeByUserId(ensureHostProperties(fixedHosts)),
      };
    }

    // Build RR vs fixed explicitly and normalize
    const fixedHosts = ensureHostProperties(normalizedHosts.filter((h) => h.isFixed === true));
    const roundRobinHosts = ensureHostProperties(normalizedHosts.filter((h) => h.isFixed !== true));

    // If it is rerouting, we should not force reschedule with same host.
    const hostsAfterRescheduleWithSameRoundRobinHost = dedupeByUserId(
      ensureHostProperties(
        applyFilterWithFallback(
          roundRobinHosts,
          await this.dependencies.filterHostsService.filterHostsBySameRoundRobinHost({
            hosts: roundRobinHosts,
            rescheduleUid,
            rescheduleWithSameRoundRobinHost: eventType.rescheduleWithSameRoundRobinHost,
            routedTeamMemberIds,
          })
        )
      )
    );

    if (hostsAfterRescheduleWithSameRoundRobinHost.length === 1) {
      return {
        qualifiedRRHosts: dedupeByUserId(ensureHostProperties(hostsAfterRescheduleWithSameRoundRobinHost)),
        fixedHosts: dedupeByUserId(ensureHostProperties(fixedHosts)),
      };
    }

    const hostsAfterSegmentMatching = dedupeByUserId(
      ensureHostProperties(
        applyFilterWithFallback(
          hostsAfterRescheduleWithSameRoundRobinHost,
          (await findMatchingHostsWithEventSegment({
            eventType,
            hosts: hostsAfterRescheduleWithSameRoundRobinHost,
          })) as typeof hostsAfterRescheduleWithSameRoundRobinHost
        )
      )
    );

    if (hostsAfterSegmentMatching.length === 1) {
      return {
        qualifiedRRHosts: dedupeByUserId(ensureHostProperties(hostsAfterSegmentMatching)),
        fixedHosts: dedupeByUserId(ensureHostProperties(fixedHosts)),
      };
    }

    //if segment matching doesn't return any hosts we fall back to all round robin hosts
    const officalRRHosts = hostsAfterSegmentMatching.length
      ? hostsAfterSegmentMatching
      : hostsAfterRescheduleWithSameRoundRobinHost;

    const hostsAfterContactOwnerMatching = dedupeByUserId(
      ensureHostProperties(
        applyFilterWithFallback(
          officalRRHosts,
          officalRRHosts.filter((host) => host.user.email === contactOwnerEmail)
        )
      )
    );

    const hostsAfterRoutedTeamMemberIdsMatching = dedupeByUserId(
      ensureHostProperties(
        applyFilterWithFallback(
          officalRRHosts,
          officalRRHosts.filter((host) => routedTeamMemberIds.includes(host.user.id))
        )
      )
    );

    if (hostsAfterRoutedTeamMemberIdsMatching.length === 1) {
      if (hostsAfterContactOwnerMatching.length === 1) {
        return {
          qualifiedRRHosts: dedupeByUserId(ensureHostProperties(hostsAfterContactOwnerMatching)),
          allFallbackRRHosts: dedupeByUserId(
            ensureHostProperties(
              getFallBackWithContactOwner(
                hostsAfterRoutedTeamMemberIdsMatching,
                hostsAfterContactOwnerMatching[0]
              )
            )
          ),
          fixedHosts: dedupeByUserId(ensureHostProperties(fixedHosts)),
        };
      }
      return {
        qualifiedRRHosts: dedupeByUserId(ensureHostProperties(hostsAfterRoutedTeamMemberIdsMatching)),
        fixedHosts: dedupeByUserId(ensureHostProperties(fixedHosts)),
      };
    }

    const hostsAfterFairnessMatching = dedupeByUserId(
      ensureHostProperties(
        applyFilterWithFallback(
          hostsAfterRoutedTeamMemberIdsMatching,
          await filterHostsByLeadThreshold({
            eventType,
            hosts: hostsAfterRoutedTeamMemberIdsMatching,
            maxLeadThreshold: eventType.maxLeadThreshold ?? null,
            routingFormResponse,
          })
        )
      )
    );

    if (hostsAfterContactOwnerMatching.length === 1) {
      return {
        qualifiedRRHosts: dedupeByUserId(ensureHostProperties(hostsAfterContactOwnerMatching)),
        allFallbackRRHosts: dedupeByUserId(
          ensureHostProperties(
            getFallBackWithContactOwner(hostsAfterFairnessMatching, hostsAfterContactOwnerMatching[0])
          )
        ),
        fixedHosts: dedupeByUserId(ensureHostProperties(fixedHosts)),
      };
    }

    return {
      qualifiedRRHosts: dedupeByUserId(ensureHostProperties(hostsAfterFairnessMatching)),
      // only if fairness filtering is active
      allFallbackRRHosts:
        hostsAfterFairnessMatching.length !== hostsAfterRoutedTeamMemberIdsMatching.length
          ? dedupeByUserId(ensureHostProperties(hostsAfterRoutedTeamMemberIdsMatching))
          : undefined,
      fixedHosts: dedupeByUserId(ensureHostProperties(fixedHosts)),
    };
  }

  findQualifiedHostsWithDelegationCredentials = withReporting(
    this._findQualifiedHostsWithDelegationCredentials.bind(this),
    "findQualifiedHostsWithDelegationCredentials"
  );
}

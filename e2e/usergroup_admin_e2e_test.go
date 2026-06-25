//go:build e2e
// +build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestUserGroupAdminRoutesAndPrincipalAVUsE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()
	zone := e2eIRODSZone(t)

	adminUser := e2eBasicUsername(t)
	if adminUser != "test1" {
		t.Fatalf("expected IrodsPrimaryTestUser to be test1 for this admin e2e test, got %q", adminUser)
	}

	userName := "e2euguser" + randomToken(nil, 8)
	groupName := "e2euggrp" + randomToken(nil, 8)
	userPassword := "e2eUserPass-" + randomToken(nil, 12)
	userAttr := "e2e.user.avu." + randomToken(nil, 8)
	groupAttr := "e2e.group.avu." + randomToken(nil, 8)

	cleanupUserGroupAdminSubjectsE2E(t, client, baseURL, userName, groupName)
	t.Cleanup(func() {
		cleanupUserGroupAdminSubjectsE2E(t, client, baseURL, userName, groupName)
	})

	userCollectionURL := userGroupAdminURL(baseURL, "/api/v1/user", zone)
	userURL := userGroupAdminURL(baseURL, "/api/v1/user/"+url.PathEscape(userName), zone)
	groupCollectionURL := userGroupAdminURL(baseURL, "/api/v1/usergroup", zone)
	groupURL := userGroupAdminURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName), zone)
	memberCollectionURL := userGroupAdminURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName)+"/member", zone)
	memberURL := userGroupAdminURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName)+"/member/"+url.PathEscape(userName), zone)
	userAVUCollectionURL := userGroupAdminURL(baseURL, "/api/v1/user/"+url.PathEscape(userName)+"/avu", zone)
	groupAVUCollectionURL := userGroupAdminURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName)+"/avu", zone)

	status, body := requestUserGroupAdminE2E(t, client, http.MethodPost, userCollectionURL, map[string]any{
		"name":     userName,
		"type":     "rodsuser",
		"password": userPassword,
	})
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusCreated)
	user := decodeUserAdminUserResponseE2E(t, body).User
	assertUserAdminUserE2E(t, user, userName, zone, "rodsuser")
	assertUserAdminActionLinkE2E(t, user.Links.AVUs, http.MethodGet, "/api/v1/user/"+userName+"/avu")
	assertUserAdminActionLinkE2E(t, user.Links.CreateAVU, http.MethodPost, "/api/v1/user/"+userName+"/avu")

	status, body = requestUserGroupAdminE2E(t, client, http.MethodGet, userURL, nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusOK)
	assertUserAdminUserE2E(t, decodeUserAdminUserResponseE2E(t, body).User, userName, zone, "rodsuser")

	status, body = requestUserGroupAdminE2E(t, client, http.MethodGet, userCollectionURL+"&prefix="+url.QueryEscape(userName[:6]), nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusOK)
	assertUserAdminUserListedE2E(t, body, userName)

	status, body = requestUserGroupAdminE2E(t, client, http.MethodPost, groupCollectionURL, map[string]any{
		"name": groupName,
	})
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusCreated)
	group := decodeUserAdminGroupResponseE2E(t, body).Group
	assertUserAdminGroupE2E(t, group, groupName, zone)
	assertUserAdminActionLinkE2E(t, group.Links.AVUs, http.MethodGet, "/api/v1/usergroup/"+groupName+"/avu")
	assertUserAdminActionLinkE2E(t, group.Links.CreateAVU, http.MethodPost, "/api/v1/usergroup/"+groupName+"/avu")

	status, body = requestUserGroupAdminE2E(t, client, http.MethodGet, groupCollectionURL+"&prefix="+url.QueryEscape(groupName[:6]), nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusOK)
	assertUserAdminGroupListedE2E(t, body, groupName)

	status, body = requestUserGroupAdminE2E(t, client, http.MethodPost, memberCollectionURL, map[string]any{
		"user_name": userName,
	})
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusOK)
	assertUserAdminGroupMemberE2E(t, decodeUserAdminGroupResponseE2E(t, body).Group, userName, true)

	status, body = requestUserGroupAdminE2E(t, client, http.MethodGet, groupURL, nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusOK)
	assertUserAdminGroupMemberE2E(t, decodeUserAdminGroupResponseE2E(t, body).Group, userName, true)

	status, body = requestUserGroupAdminE2E(t, client, http.MethodGet, userGroupAdminURL(baseURL, "/api/v1/user/"+url.PathEscape(userName)+"/usergroup", zone), nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusOK)
	assertUserAdminGroupRefListedE2E(t, body, groupName)

	userAVU := exercisePrincipalAVUE2E(t, client, userAVUCollectionURL, userAttr)
	groupAVU := exercisePrincipalAVUE2E(t, client, groupAVUCollectionURL, groupAttr)

	status, body = requestUserGroupAdminE2E(t, client, http.MethodDelete, e2eHrefURL(baseURL, userAVU.Links.Delete.Href), nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusNoContent)
	status, body = requestUserGroupAdminE2E(t, client, http.MethodDelete, e2eHrefURL(baseURL, groupAVU.Links.Delete.Href), nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusNoContent)

	status, body = requestUserGroupAdminE2E(t, client, http.MethodDelete, memberURL, nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusOK)
	assertUserAdminGroupMemberE2E(t, decodeUserAdminGroupResponseE2E(t, body).Group, userName, false)

	status, body = requestUserGroupAdminE2E(t, client, http.MethodDelete, groupURL, nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusNoContent)
	status, body = requestUserGroupAdminE2E(t, client, http.MethodDelete, userURL, nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusNoContent)
}

func TestCreateRodsUserThenPromoteToGroupAdminRejectsSelfTypeChangesE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()
	zone := e2eIRODSZone(t)

	adminUser := e2eBasicUsername(t)
	if adminUser != "test1" {
		t.Fatalf("expected IrodsPrimaryTestUser to be test1 for this admin e2e test, got %q", adminUser)
	}

	userName := "e2egroupadmin" + randomToken(nil, 8)
	userPassword := "e2eGroupAdminPass-" + randomToken(nil, 12)
	managedUserName := "e2egamember" + randomToken(nil, 8)
	managedUserPassword := "e2eManagedUserPass-" + randomToken(nil, 12)
	groupName := "e2egagrp" + randomToken(nil, 8)
	cleanupUserGroupAdminSubjectsE2E(t, client, baseURL, userName, "")
	cleanupUserGroupAdminSubjectsE2E(t, client, baseURL, managedUserName, groupName)
	t.Cleanup(func() {
		cleanupUserGroupAdminSubjectsE2E(t, client, baseURL, managedUserName, groupName)
		cleanupUserGroupAdminSubjectsE2E(t, client, baseURL, userName, "")
	})

	userCollectionURL := userGroupAdminURL(baseURL, "/api/v1/user", zone)
	userURL := userGroupAdminURL(baseURL, "/api/v1/user/"+url.PathEscape(userName), zone)
	userTypeURL := userGroupAdminURL(baseURL, "/api/v1/user/"+url.PathEscape(userName)+"/type", zone)
	managedUserURL := userGroupAdminURL(baseURL, "/api/v1/user/"+url.PathEscape(managedUserName), zone)
	groupCollectionURL := userGroupAdminURL(baseURL, "/api/v1/usergroup", zone)
	groupURL := userGroupAdminURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName), zone)
	memberCollectionURL := userGroupAdminURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName)+"/member", zone)
	memberURL := userGroupAdminURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName)+"/member/"+url.PathEscape(managedUserName), zone)

	status, body := requestUserGroupAdminE2E(t, client, http.MethodPost, userCollectionURL, map[string]any{
		"name":     userName,
		"type":     "rodsuser",
		"password": userPassword,
	})
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusCreated)
	assertUserAdminUserE2E(t, decodeUserAdminUserResponseE2E(t, body).User, userName, zone, "rodsuser")

	status, body = requestUserGroupAdminE2E(t, client, http.MethodPut, userTypeURL, map[string]any{
		"type": "groupadmin",
	})
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusOK)
	assertUserAdminUserE2E(t, decodeUserAdminUserResponseE2E(t, body).User, userName, zone, "groupadmin")

	status, body = requestUserGroupAdminE2E(t, client, http.MethodGet, userURL, nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusOK)
	assertUserAdminUserE2E(t, decodeUserAdminUserResponseE2E(t, body).User, userName, zone, "groupadmin")

	membershipSummaryURL := userGroupAdminURL(baseURL, "/api/v1/user/membership-summary", zone) + "&prefix=" + url.QueryEscape(userName) + "&limit=10"
	status, body = requestUserGroupAdminE2E(t, client, http.MethodGet, membershipSummaryURL, nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusOK)
	if !strings.Contains(body, `"name":"`+userName+`"`) || !strings.Contains(body, `"type":"groupadmin"`) {
		t.Fatalf("expected membership summary to include promoted groupadmin user %q, got %s", userName, body)
	}

	status, body = requestUserGroupAdminE2EAs(t, client, http.MethodPost, userCollectionURL, map[string]any{
		"name":     managedUserName,
		"type":     "rodsuser",
		"password": managedUserPassword,
	}, userName, userPassword)
	requireUserGroupAdminStatusForRequestE2E(t, http.MethodPost, userCollectionURL, status, body, http.StatusCreated)
	assertUserAdminUserE2E(t, decodeUserAdminUserResponseE2E(t, body).User, managedUserName, zone, "rodsuser")

	currentUserURL := userGroupAdminURL(baseURL, "/api/v1/user/me", zone)
	status, body = requestUserGroupAdminE2EAs(t, client, http.MethodGet, currentUserURL, nil, managedUserName, managedUserPassword)
	requireUserGroupAdminStatusForRequestE2E(t, http.MethodGet, currentUserURL, status, body, http.StatusOK)
	if !strings.Contains(body, `"name":"`+managedUserName+`"`) || !strings.Contains(body, `"type":"rodsuser"`) {
		t.Fatalf("expected created rodsuser %q to authenticate with initial password, got %s", managedUserName, body)
	}

	status, body = requestUserGroupAdminE2EAs(t, client, http.MethodDelete, managedUserURL, nil, userName, userPassword)
	requireUserGroupAdminStatusForRequestE2E(t, http.MethodDelete, managedUserURL, status, body, http.StatusForbidden)

	status, body = requestUserGroupAdminE2EAs(t, client, http.MethodPost, groupCollectionURL, map[string]any{
		"name": groupName,
	}, userName, userPassword)
	requireUserGroupAdminStatusForRequestE2E(t, http.MethodPost, groupCollectionURL, status, body, http.StatusCreated)
	assertUserAdminGroupE2E(t, decodeUserAdminGroupResponseE2E(t, body).Group, groupName, zone)

	status, body = requestUserGroupAdminE2EAs(t, client, http.MethodPost, memberCollectionURL, map[string]any{
		"user_name": managedUserName,
	}, userName, userPassword)
	requireUserGroupAdminStatusForRequestE2E(t, http.MethodPost, memberCollectionURL, status, body, http.StatusOK)
	assertUserAdminGroupMemberE2E(t, decodeUserAdminGroupResponseE2E(t, body).Group, managedUserName, true)

	status, body = requestUserGroupAdminE2EAs(t, client, http.MethodDelete, memberURL, nil, userName, userPassword)
	requireUserGroupAdminStatusForRequestE2E(t, http.MethodDelete, memberURL, status, body, http.StatusOK)
	assertUserAdminGroupMemberE2E(t, decodeUserAdminGroupResponseE2E(t, body).Group, managedUserName, false)

	status, body = requestUserGroupAdminE2EAs(t, client, http.MethodDelete, groupURL, nil, userName, userPassword)
	requireUserGroupAdminStatusForRequestE2E(t, http.MethodDelete, groupURL, status, body, http.StatusForbidden)

	status, body = requestUserGroupAdminE2E(t, client, http.MethodDelete, groupURL, nil)
	requireUserGroupAdminStatusForRequestE2E(t, http.MethodDelete, groupURL, status, body, http.StatusNoContent)

	status, body = requestUserGroupAdminE2EAs(t, client, http.MethodPut, userTypeURL, map[string]any{
		"type": "rodsadmin",
	}, userName, userPassword)
	requireUserGroupAdminStatusForRequestE2E(t, http.MethodPut, userTypeURL, status, body, http.StatusForbidden)

	status, body = requestUserGroupAdminE2EAs(t, client, http.MethodPut, userTypeURL, map[string]any{
		"type": "rodsuser",
	}, userName, userPassword)
	requireUserGroupAdminStatusForRequestE2E(t, http.MethodPut, userTypeURL, status, body, http.StatusForbidden)

	status, body = requestUserGroupAdminE2E(t, client, http.MethodDelete, userURL, nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusNoContent)
}

func TestUserGroupSummaryAndPrincipalSearchRoutesE2E(t *testing.T) {
	token := requireE2EBearerToken(t)
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()
	zone := e2eIRODSZone(t)

	requests := []string{
		userGroupAdminURL(baseURL, "/api/v1/user/me", zone),
		userGroupAdminURL(baseURL, "/api/v1/usergroup/summary", zone) + "&limit=10",
		userGroupAdminURL(baseURL, "/api/v1/user/membership-summary", zone) + "&limit=10",
		userGroupAdminURL(baseURL, "/api/v1/principal", zone) + "&query=" + url.QueryEscape("test") + "&kind=user&kind=group&limit=10",
	}

	for _, requestURL := range requests {
		req := newE2ERequest(t, http.MethodGet, requestURL, nil)
		setBearerAuth(req, token)

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("perform GET %s: %v", requestURL, err)
		}
		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			t.Fatalf("read GET %s response body: %v", requestURL, readErr)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 for GET %s, got %d: %s", requestURL, resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
		}
		assertUserGroupAdminSelfLinkE2E(t, string(bodyBytes))
	}
}

type userGroupAdminActionLinkE2E struct {
	Href   string `json:"href"`
	Method string `json:"method"`
}

type userGroupAdminUserLinksE2E struct {
	Self      *userGroupAdminActionLinkE2E `json:"self"`
	Update    *userGroupAdminActionLinkE2E `json:"update"`
	Delete    *userGroupAdminActionLinkE2E `json:"delete"`
	AVUs      *userGroupAdminActionLinkE2E `json:"avus"`
	CreateAVU *userGroupAdminActionLinkE2E `json:"create_avu"`
}

type userGroupAdminUserE2E struct {
	Name  string                     `json:"name"`
	Zone  string                     `json:"zone"`
	Type  string                     `json:"type"`
	Links userGroupAdminUserLinksE2E `json:"links"`
}

type userGroupAdminGroupLinksE2E struct {
	Self      *userGroupAdminActionLinkE2E `json:"self"`
	Delete    *userGroupAdminActionLinkE2E `json:"delete"`
	AddMember *userGroupAdminActionLinkE2E `json:"add_member"`
	AVUs      *userGroupAdminActionLinkE2E `json:"avus"`
	CreateAVU *userGroupAdminActionLinkE2E `json:"create_avu"`
}

type userGroupAdminGroupMemberLinksE2E struct {
	Self            *userGroupAdminActionLinkE2E `json:"self"`
	RemoveFromGroup *userGroupAdminActionLinkE2E `json:"remove_from_group"`
}

type userGroupAdminGroupMemberE2E struct {
	Name  string                            `json:"name"`
	Zone  string                            `json:"zone"`
	Type  string                            `json:"type"`
	Links userGroupAdminGroupMemberLinksE2E `json:"links"`
}

type userGroupAdminGroupE2E struct {
	Name    string                         `json:"name"`
	Zone    string                         `json:"zone"`
	Type    string                         `json:"type"`
	Members []userGroupAdminGroupMemberE2E `json:"members"`
	Links   userGroupAdminGroupLinksE2E    `json:"links"`
}

type userGroupAdminAVULinksE2E struct {
	Update *userGroupAdminActionLinkE2E `json:"update"`
	Delete *userGroupAdminActionLinkE2E `json:"delete"`
}

type userGroupAdminAVUE2E struct {
	ID     string                    `json:"id"`
	Attrib string                    `json:"attrib"`
	Value  string                    `json:"value"`
	Unit   string                    `json:"unit"`
	Links  userGroupAdminAVULinksE2E `json:"links"`
}

func requestUserGroupAdminE2E(t *testing.T, client *http.Client, method string, requestURL string, payload any) (int, string) {
	t.Helper()
	return requestUserGroupAdminE2EAs(t, client, method, requestURL, payload, e2eBasicUsername(t), e2eBasicPassword(t))
}

func requestUserGroupAdminE2EAs(t *testing.T, client *http.Client, method string, requestURL string, payload any, username string, password string) (int, string) {
	t.Helper()

	var bodyReader io.Reader
	if payload != nil {
		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal %s request payload: %v", method, err)
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	req := newE2ERequest(t, method, requestURL, bodyReader)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	setBasicAuthCredentials(req, username, password)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform %s %s: %v", method, requestURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s response body: %v", method, err)
	}

	return resp.StatusCode, string(body)
}

func cleanupUserGroupAdminSubjectsE2E(t *testing.T, client *http.Client, baseURL string, userName string, groupName string) {
	t.Helper()

	zone := e2eIRODSZone(t)

	memberURL := userGroupAdminURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName)+"/member/"+url.PathEscape(userName), zone)
	userURL := userGroupAdminURL(baseURL, "/api/v1/user/"+url.PathEscape(userName), zone)
	groupURL := userGroupAdminURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName), zone)

	status, body := requestUserGroupAdminE2E(t, client, http.MethodDelete, memberURL, nil)
	if status != http.StatusOK && status != http.StatusNotFound {
		t.Logf("cleanup remove member got %d: %s", status, strings.TrimSpace(body))
	}
	status, body = requestUserGroupAdminE2E(t, client, http.MethodDelete, userURL, nil)
	if status != http.StatusNoContent && status != http.StatusNotFound {
		t.Logf("cleanup delete user got %d: %s", status, strings.TrimSpace(body))
	}
	status, body = requestUserGroupAdminE2E(t, client, http.MethodDelete, groupURL, nil)
	if status != http.StatusNoContent && status != http.StatusNotFound {
		t.Logf("cleanup delete group got %d: %s", status, strings.TrimSpace(body))
	}
}

func exercisePrincipalAVUE2E(t *testing.T, client *http.Client, collectionURL string, attr string) userGroupAdminAVUE2E {
	t.Helper()

	status, body := requestUserGroupAdminE2E(t, client, http.MethodPost, collectionURL, map[string]any{
		"attrib": attr,
		"value":  "before",
		"unit":   "e2e",
	})
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusCreated)
	created := decodeUserGroupAdminAVUResponseE2E(t, body).AVU
	assertUserGroupAdminAVUE2E(t, created, attr, "before", "e2e")
	assertUserAdminActionLinkE2E(t, created.Links.Update, http.MethodPut, "/avu/"+created.ID)
	assertUserAdminActionLinkE2E(t, created.Links.Delete, http.MethodDelete, "/avu/"+created.ID)

	status, body = requestUserGroupAdminE2E(t, client, http.MethodGet, collectionURL+"&attrib="+url.QueryEscape(attr), nil)
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusOK)
	assertUserGroupAdminAVUListedE2E(t, body, created.ID, attr, "before")

	status, body = requestUserGroupAdminE2E(t, client, http.MethodPut, e2eHrefURL(requireE2EBaseURL(t), created.Links.Update.Href), map[string]any{
		"attrib": attr,
		"value":  "after",
		"unit":   "e2e-updated",
	})
	requireUserGroupAdminStatusE2E(t, status, body, http.StatusOK)
	updated := decodeUserGroupAdminAVUResponseE2E(t, body).AVU
	assertUserGroupAdminAVUE2E(t, updated, attr, "after", "e2e-updated")

	return updated
}

func userGroupAdminURL(baseURL string, path string, zone string) string {
	return strings.TrimRight(baseURL, "/") + path + "?" + userGroupAdminQuery(zone).Encode()
}

func userGroupAdminQuery(zone string) url.Values {
	query := url.Values{}
	if strings.TrimSpace(zone) != "" {
		query.Set("zone", strings.TrimSpace(zone))
	}
	return query
}

func requireUserGroupAdminStatusE2E(t *testing.T, got int, body string, expected ...int) {
	t.Helper()

	for _, status := range expected {
		if got == status {
			return
		}
	}
	t.Fatalf("expected status %v, got %d: %s", expected, got, strings.TrimSpace(body))
}

func requireUserGroupAdminStatusForRequestE2E(t *testing.T, method string, requestURL string, got int, body string, expected ...int) {
	t.Helper()

	for _, status := range expected {
		if got == status {
			return
		}
	}
	t.Fatalf("expected status %v for %s %s, got %d: %s", expected, method, requestURL, got, strings.TrimSpace(body))
}

func decodeUserAdminUserResponseE2E(t *testing.T, body string) struct {
	User userGroupAdminUserE2E `json:"user"`
} {
	t.Helper()

	var payload struct {
		User userGroupAdminUserE2E `json:"user"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decode user response: %v: %s", err, strings.TrimSpace(body))
	}
	return payload
}

func decodeUserAdminGroupResponseE2E(t *testing.T, body string) struct {
	Group userGroupAdminGroupE2E `json:"group"`
} {
	t.Helper()

	var payload struct {
		Group userGroupAdminGroupE2E `json:"group"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decode group response: %v: %s", err, strings.TrimSpace(body))
	}
	return payload
}

func decodeUserGroupAdminAVUResponseE2E(t *testing.T, body string) struct {
	AVU userGroupAdminAVUE2E `json:"avu"`
} {
	t.Helper()

	var payload struct {
		AVU userGroupAdminAVUE2E `json:"avu"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decode AVU response: %v: %s", err, strings.TrimSpace(body))
	}
	return payload
}

func assertUserAdminUserE2E(t *testing.T, user userGroupAdminUserE2E, expectedName string, expectedZone string, expectedType string) {
	t.Helper()

	if user.Name != expectedName || user.Zone != expectedZone || user.Type != expectedType {
		t.Fatalf("expected user %s#%s type %s, got %+v", expectedZone, expectedName, expectedType, user)
	}
	assertUserAdminActionLinkE2E(t, user.Links.Self, http.MethodGet, "/api/v1/user/"+expectedName)
	assertUserAdminActionLinkE2E(t, user.Links.Update, http.MethodPut, "/api/v1/user/"+expectedName)
	assertUserAdminActionLinkE2E(t, user.Links.Delete, http.MethodDelete, "/api/v1/user/"+expectedName)
}

func assertUserAdminGroupE2E(t *testing.T, group userGroupAdminGroupE2E, expectedName string, expectedZone string) {
	t.Helper()

	if group.Name != expectedName || group.Zone != expectedZone || group.Type != "rodsgroup" {
		t.Fatalf("expected group %s#%s type rodsgroup, got %+v", expectedZone, expectedName, group)
	}
	assertUserAdminActionLinkE2E(t, group.Links.Self, http.MethodGet, "/api/v1/usergroup/"+expectedName)
	assertUserAdminActionLinkE2E(t, group.Links.Delete, http.MethodDelete, "/api/v1/usergroup/"+expectedName)
	assertUserAdminActionLinkE2E(t, group.Links.AddMember, http.MethodPost, "/api/v1/usergroup/"+expectedName+"/member")
}

func assertUserAdminGroupMemberE2E(t *testing.T, group userGroupAdminGroupE2E, expectedMember string, wantPresent bool) {
	t.Helper()

	for _, member := range group.Members {
		if member.Name != expectedMember {
			continue
		}
		if !wantPresent {
			t.Fatalf("expected member %q to be absent from group %q", expectedMember, group.Name)
		}
		assertUserAdminActionLinkE2E(t, member.Links.Self, http.MethodGet, "/api/v1/user/"+expectedMember)
		assertUserAdminActionLinkE2E(t, member.Links.RemoveFromGroup, http.MethodDelete, "/api/v1/usergroup/"+group.Name+"/member/"+expectedMember)
		return
	}

	if wantPresent {
		t.Fatalf("expected member %q to be present in group %q; members=%+v", expectedMember, group.Name, group.Members)
	}
}

func assertUserAdminActionLinkE2E(t *testing.T, link *userGroupAdminActionLinkE2E, expectedMethod string, expectedPathPart string) {
	t.Helper()

	if link == nil {
		t.Fatalf("expected %s link containing %q, got nil", expectedMethod, expectedPathPart)
	}
	if link.Method != expectedMethod {
		t.Fatalf("expected link method %s, got %s for %+v", expectedMethod, link.Method, link)
	}
	if !strings.Contains(link.Href, expectedPathPart) {
		t.Fatalf("expected link href %q to contain %q", link.Href, expectedPathPart)
	}
}

func assertUserAdminUserListedE2E(t *testing.T, body string, expectedName string) {
	t.Helper()

	var payload struct {
		Users []userGroupAdminUserE2E `json:"users"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decode users response: %v: %s", err, strings.TrimSpace(body))
	}
	for _, user := range payload.Users {
		if user.Name == expectedName {
			assertUserAdminActionLinkE2E(t, user.Links.AVUs, http.MethodGet, "/api/v1/user/"+expectedName+"/avu")
			assertUserAdminActionLinkE2E(t, user.Links.CreateAVU, http.MethodPost, "/api/v1/user/"+expectedName+"/avu")
			return
		}
	}
	t.Fatalf("expected user %q in list response: %s", expectedName, body)
}

func assertUserAdminGroupListedE2E(t *testing.T, body string, expectedName string) {
	t.Helper()

	var payload struct {
		Groups []userGroupAdminGroupE2E `json:"groups"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decode groups response: %v: %s", err, strings.TrimSpace(body))
	}
	for _, group := range payload.Groups {
		if group.Name == expectedName {
			assertUserAdminActionLinkE2E(t, group.Links.AVUs, http.MethodGet, "/api/v1/usergroup/"+expectedName+"/avu")
			assertUserAdminActionLinkE2E(t, group.Links.CreateAVU, http.MethodPost, "/api/v1/usergroup/"+expectedName+"/avu")
			return
		}
	}
	t.Fatalf("expected group %q in list response: %s", expectedName, body)
}

func assertUserAdminGroupRefListedE2E(t *testing.T, body string, expectedName string) {
	t.Helper()

	var payload struct {
		Groups []struct {
			Name string `json:"name"`
		} `json:"groups"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decode user group refs response: %v: %s", err, strings.TrimSpace(body))
	}
	for _, group := range payload.Groups {
		if group.Name == expectedName {
			return
		}
	}
	t.Fatalf("expected group %q in reverse membership response: %s", expectedName, body)
}

func assertUserGroupAdminAVUE2E(t *testing.T, avu userGroupAdminAVUE2E, expectedAttr string, expectedValue string, expectedUnit string) {
	t.Helper()

	if strings.TrimSpace(avu.ID) == "" {
		t.Fatalf("expected AVU id to be populated: %+v", avu)
	}
	if avu.Attrib != expectedAttr || avu.Value != expectedValue || avu.Unit != expectedUnit {
		t.Fatalf("expected AVU %q=%q %q, got %+v", expectedAttr, expectedValue, expectedUnit, avu)
	}
	assertUserAdminActionLinkE2E(t, avu.Links.Update, http.MethodPut, "/avu/"+avu.ID)
	assertUserAdminActionLinkE2E(t, avu.Links.Delete, http.MethodDelete, "/avu/"+avu.ID)
}

func assertUserGroupAdminAVUListedE2E(t *testing.T, body string, expectedID string, expectedAttr string, expectedValue string) {
	t.Helper()

	var payload struct {
		Links struct {
			Self   *userGroupAdminActionLinkE2E `json:"self"`
			Create *userGroupAdminActionLinkE2E `json:"create"`
		} `json:"links"`
		AVUs []userGroupAdminAVUE2E `json:"avus"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decode AVU list response: %v: %s", err, strings.TrimSpace(body))
	}
	assertUserAdminActionLinkE2E(t, payload.Links.Self, http.MethodGet, "/avu")
	assertUserAdminActionLinkE2E(t, payload.Links.Create, http.MethodPost, "/avu")
	for _, avu := range payload.AVUs {
		if avu.ID == expectedID {
			if avu.Attrib != expectedAttr || avu.Value != expectedValue {
				t.Fatalf("expected listed AVU %q attr/value %q/%q, got %+v", expectedID, expectedAttr, expectedValue, avu)
			}
			assertUserAdminActionLinkE2E(t, avu.Links.Update, http.MethodPut, "/avu/"+expectedID)
			assertUserAdminActionLinkE2E(t, avu.Links.Delete, http.MethodDelete, "/avu/"+expectedID)
			return
		}
	}
	t.Fatalf("expected AVU id %q in list response: %s", expectedID, body)
}

func assertUserGroupAdminSelfLinkE2E(t *testing.T, body string) {
	t.Helper()

	var payload struct {
		Links struct {
			Self *userGroupAdminActionLinkE2E `json:"self"`
		} `json:"links"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decode response links: %v: %s", err, strings.TrimSpace(body))
	}
	assertUserAdminActionLinkE2E(t, payload.Links.Self, http.MethodGet, "/api/v1/")
}

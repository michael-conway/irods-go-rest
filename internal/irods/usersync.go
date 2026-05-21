package irods

import (
	"errors"
	"fmt"

	usersyncext "github.com/michael-conway/go-irodsclient-extensions/usersync"
	"github.com/michael-conway/irods-go-rest/internal/domain"
)

func mapUserSyncError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, usersyncext.ErrNotFound):
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	case errors.Is(err, usersyncext.ErrPermissionDenied):
		return fmt.Errorf("%w: %v", ErrPermissionDenied, err)
	case errors.Is(err, usersyncext.ErrConflict):
		return fmt.Errorf("%w: %v", ErrConflict, err)
	default:
		return err
	}
}

func mapUserSyncUser(user usersyncext.User) domain.User {
	return domain.User{
		ID:   user.ID,
		Name: user.Name,
		Zone: user.Zone,
		Type: user.Type,
	}
}

func mapUserSyncGroup(group usersyncext.Group) domain.UserGroup {
	return domain.UserGroup{
		ID:      group.ID,
		Name:    group.Name,
		Zone:    group.Zone,
		Type:    group.Type,
		Members: mapUserSyncMembers(group.Members),
	}
}

func mapUserSyncMembers(members []usersyncext.GroupMember) []domain.UserGroupMember {
	if len(members) == 0 {
		return []domain.UserGroupMember{}
	}

	result := make([]domain.UserGroupMember, 0, len(members))
	for _, member := range members {
		result = append(result, domain.UserGroupMember{
			ID:   member.ID,
			Name: member.Name,
			Zone: member.Zone,
			Type: member.Type,
		})
	}
	return result
}

package irods

import (
	"fmt"
	"strings"

	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
)

func authenticatedPrincipalType(filesystem CatalogFileSystem, requestContext *RequestContext, zone string, operation string, allowedTypes ...irodstypes.IRODSUserType) (irodstypes.IRODSUserType, error) {
	username := strings.TrimSpace(safeUsername(requestContext))
	zone = strings.TrimSpace(zone)
	if username == "" {
		return "", permissionDeniedForAllowedTypes(operation, allowedTypes...)
	}

	user, err := filesystem.GetUser(username, zone, "")
	if err != nil {
		return "", permissionDeniedForAllowedTypes(operation, allowedTypes...)
	}
	if user == nil || strings.TrimSpace(user.Name) != username || strings.TrimSpace(user.Zone) != zone {
		return "", permissionDeniedForAllowedTypes(operation, allowedTypes...)
	}

	for _, allowedType := range allowedTypes {
		if user.Type == allowedType {
			return user.Type, nil
		}
	}

	return "", permissionDeniedForAllowedTypes(operation, allowedTypes...)
}

func permissionDeniedForAllowedTypes(operation string, allowedTypes ...irodstypes.IRODSUserType) error {
	return fmt.Errorf("%w: %s requires %s", ErrPermissionDenied, operation, allowedTypeDescription(allowedTypes...))
}

func allowedTypeDescription(allowedTypes ...irodstypes.IRODSUserType) string {
	labels := make([]string, 0, len(allowedTypes))
	for _, allowedType := range allowedTypes {
		switch allowedType {
		case irodstypes.IRODSUserRodsAdmin:
			labels = append(labels, "rodsadmin")
		case irodstypes.IRODSUserGroupAdmin:
			labels = append(labels, "groupadmin")
		case irodstypes.IRODSUserRodsUser:
			labels = append(labels, "rodsuser")
		}
	}

	switch len(labels) {
	case 0:
		return "an authorized iRODS user"
	case 1:
		return labels[0]
	case 2:
		return labels[0] + " or " + labels[1]
	default:
		return strings.Join(labels[:len(labels)-1], ", ") + ", or " + labels[len(labels)-1]
	}
}

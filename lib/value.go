package lib

import (
	"context"
	"slices"
	"strings"

	"github.com/oarkflow/dipper"
	"github.com/oarkflow/expr"
)

func GetValue(c context.Context, v string, data map[string]any) (key string, val any) {
	key, val = getVal(c, v, data)
	if val == nil {
		if strings.Contains(v, "+") {
			vPartsG := strings.Split(v, "+")
			var value []string
			for _, v := range vPartsG {
				key, val = getVal(c, strings.TrimSpace(v), data)
				if val == nil {
					continue
				}
				value = append(value, val.(string))
			}
			val = strings.Join(value, "")
		} else {
			key, val = getVal(c, v, data)
		}
	}

	return
}

func getVal(c context.Context, v string, data map[string]any) (key string, val any) {
	var param, query, consts map[string]any
	var enums map[string]map[string]any
	headerData := make(map[string]any)
	header := c.Value("header")
	switch header := header.(type) {
	case map[string]any:
		if p, exists := header["param"]; exists && p != nil {
			param = p.(map[string]any)
		}
		if p, exists := header["query"]; exists && p != nil {
			query = p.(map[string]any)
		}
		if p, exists := header["consts"]; exists && p != nil {
			consts = p.(map[string]any)
		}
		if p, exists := header["enums"]; exists && p != nil {
			enums = p.(map[string]map[string]any)
		}
		params := []string{"param", "query", "consts", "enums", "scopes"}
		// add other data in header, other than param, query, consts, enums to data
		for k, v := range header {
			if !slices.Contains(params, k) {
				headerData[k] = v
			}
		}
	}
	v = strings.TrimPrefix(v, "header.")
	vParts := strings.Split(v, ".")
	switch vParts[0] {
	case "body":
		v := vParts[1]
		if strings.Contains(v, "*_") {
			fieldSuffix := strings.ReplaceAll(v, "*", "")
			for k, vt := range data {
				if strings.HasSuffix(k, fieldSuffix) {
					val = vt
					key = k
				}
			}
		} else {
			if vd, ok := data[v]; ok {
				val = vd
				key = v
			}
		}
	case "param":
		v := vParts[1]
		if strings.Contains(v, "*_") {
			fieldSuffix := strings.ReplaceAll(v, "*", "")
			for k, vt := range param {
				if strings.HasSuffix(k, fieldSuffix) {
					val = vt
					key = k
				}
			}
		} else {
			if vd, ok := param[v]; ok {
				val = vd
				key = v
			}
		}
	case "query":
		v := vParts[1]
		if strings.Contains(v, "*_") {
			fieldSuffix := strings.ReplaceAll(v, "*", "")
			for k, vt := range query {
				if strings.HasSuffix(k, fieldSuffix) {
					val = vt
					key = k
				}
			}
		} else {
			if vd, ok := query[v]; ok {
				val = vd
				key = v
			}
		}
	case "eval":
		// connect string except the first one if more than two parts exist
		var v string
		if len(vParts) > 2 {
			v = strings.Join(vParts[1:], ".")
		} else {
			v = vParts[1]
		}
		// remove '{{' and '}}'
		v = v[2 : len(v)-2]

		// parse the expression
		p, err := expr.Parse(v)
		if err != nil {
			return "", nil
		}
		// evaluate the expression
		val, err := p.Eval(data)
		if err != nil {
			val, err := p.Eval(headerData)
			if err == nil {
				return v, val
			}
			return "", nil
		} else {
			return v, val
		}
	case "eval_raw", "gorm_eval":
		// connect string except the first one if more than two parts exist
		var v string
		if len(vParts) > 2 {
			v = strings.Join(vParts[1:], ".")
		} else {
			v = vParts[1]
		}
		// remove '{{' and '}}'
		v = v[2 : len(v)-2]

		// parse the expression
		p, err := expr.Parse(v)
		if err != nil {
			return "", nil
		}
		dt := map[string]any{
			"header": header,
		}
		for k, vt := range data {
			dt[k] = vt
		}
		// evaluate the expression
		val, err := p.Eval(dt)
		if err != nil {
			val, err := p.Eval(headerData)
			if err == nil {
				return v, val
			}
			return "", nil
		} else {
			return v, val
		}
	case "consts":
		constG := vParts[1]
		if constVal, ok := consts[constG]; ok {
			val = constVal
			key = v
		}
	case "enums":
		enumG := vParts[1]
		if enumGVal, ok := enums[enumG]; ok {
			if enumVal, ok := enumGVal[vParts[2]]; ok {
				val = enumVal
				key = v
			}
		}
	default:
		if strings.Contains(v, "*_") {
			fieldSuffix := strings.ReplaceAll(v, "*", "")
			for k, vt := range data {
				if strings.HasSuffix(k, fieldSuffix) {
					val = vt
					key = k
				}
			}
		} else {
			vd, err := dipper.Get(data, v)
			if err == nil {
				val = vd
				key = v
			} else {
				vd, err := dipper.Get(headerData, v)
				if err == nil {
					val = vd
					key = v
				}
			}
		}
	}
	return
}

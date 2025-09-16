import { OptionData, OptionGroupData } from 'rc-select/lib/interface/index';

import useApplicationConfigStore from '../stores/ApplicationConfigStore';

const actionNameComparisonRegEx = /ActionName\s?(=|!)=\s['"]?[a-z_]*?$/;
const actionNameLookupRegEx = /[a-z_]*$/;

export const filterAutoComplete = (inputValue: string, option?: OptionData | OptionGroupData): boolean => {
  if (option == null) return false;
  const { knownActionNames } = useApplicationConfigStore.getState();
  const optionString = String(option.value);

  const isAnActionNameComparison = actionNameComparisonRegEx.test(inputValue);
  const optionIsAnActionName = knownActionNames.has(optionString);

  if (isAnActionNameComparison && optionIsAnActionName) {
    const actionNameLookupMatch = actionNameLookupRegEx.exec(inputValue);
    if (actionNameLookupMatch == null) return false;

    const actionNameLookupString = actionNameLookupMatch[0];
    return optionString.includes(actionNameLookupString);
  } else if (isAnActionNameComparison || optionIsAnActionName) {
    return false;
  }

  const currentWord = String(inputValue.split(' ').slice(-1));
  return optionString.toUpperCase().includes(currentWord.toUpperCase());
};

export const sortOptions = (inputValue: string) => {
  const upperInput = inputValue.toUpperCase();
  return (a: OptionData, b: OptionData) => {
    const upperA = String(a.value).toUpperCase();
    const upperB = String(b.value).toUpperCase();

    const isExactA = upperA === upperInput ? 0 : 1;
    const isExactB = upperB === upperInput ? 0 : 1;

    const startsWithA = upperA.startsWith(upperInput) ? 0 : 1;
    const startsWithB = upperB.startsWith(upperInput) ? 0 : 1;

    return isExactA - isExactB || startsWithA - startsWithB || upperA.localeCompare(upperB);
  };
};

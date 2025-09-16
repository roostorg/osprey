import * as React from 'react';
import { Select } from 'antd';
import { SelectValue } from 'antd/lib/select';

interface UserSelectProps {
  onSelect: (email: string) => void;
  userResolver: () => Promise<string[]>;
}

const UserSelect = ({ onSelect, userResolver }: UserSelectProps) => {
  const [userOptions, setUserOptions] = React.useState<Array<{ value: string }>>([]);

  React.useEffect(() => {
    async function getUsers() {
      const fetchedUsers = await userResolver();
      setUserOptions(fetchedUsers.map((user) => ({ value: user })));
    }

    getUsers();
  }, [userResolver]);

  const handleSelectUser = (value: SelectValue) => {
    if (typeof value !== 'string') return;
    onSelect(value);
  };

  return (
    <Select
      showSearch
      allowClear
      onClear={() => onSelect('')}
      onChange={handleSelectUser}
      placeholder="Filter by user"
      style={{ width: 200 }}
      options={userOptions}
      filterOption={(input, option) => option?.value.startsWith(input) ?? false}
    />
  );
};

export default UserSelect;
